//! Postgres numeric values in the binary format, to and from decimal strings.
//!
//! A numeric is a header of four 16-bit fields, the digit count, the weight,
//! the sign and the display scale, followed by base-10000 digits. The weight is
//! the power of 10000 of the first digit. The display scale is the number of
//! decimal digits after the point, so 1.50 keeps its trailing zero.

use crate::error::ErrorKind;

/// Decimal digits in one base-10000 digit.
const DEC_DIGITS: usize = 4;
const NBASE: i16 = 10000;

const SIGN_POS: u16 = 0x0000;
const SIGN_NEG: u16 = 0x4000;
const SIGN_NAN: u16 = 0xC000;
const SIGN_PINF: u16 = 0xD000;
const SIGN_NINF: u16 = 0xF000;

/// The largest display scale and weight Postgres accepts.
const MAX_DSCALE: usize = 0x3FFF;
const MAX_WEIGHT: i64 = i16::MAX as i64;

/// The four 16-bit fields at the start of every numeric value.
#[derive(Debug, Clone, Copy, PartialEq)]
struct NumericHeader {
    /// How many base-10000 digits follow the header.
    ndigits: u16,
    /// The power of 10000 of the first digit.
    weight: i16,
    /// SIGN_POS or SIGN_NEG, or SIGN_NAN, SIGN_PINF or SIGN_NINF for a value
    /// with no digits.
    sign: u16,
    /// How many decimal digits are shown after the point.
    dscale: u16,
}

impl NumericHeader {
    const LEN: usize = 8;

    /// The header of NaN or an infinity, which has no digits.
    fn special(sign: u16) -> Self {
        Self {
            ndigits: 0,
            weight: 0,
            sign,
            dscale: 0,
        }
    }

    fn decode(data: &[u8]) -> Result<Self, ErrorKind> {
        if data.len() < Self::LEN {
            return Err(decode_error("the value is shorter than its header"));
        }
        Ok(Self {
            ndigits: read_u16(data, 0),
            weight: read_u16(data, 2) as i16,
            sign: read_u16(data, 4),
            dscale: read_u16(data, 6),
        })
    }

    fn encode(&self, out: &mut Vec<u8>) {
        out.extend_from_slice(&self.ndigits.to_be_bytes());
        out.extend_from_slice(&self.weight.to_be_bytes());
        out.extend_from_slice(&self.sign.to_be_bytes());
        out.extend_from_slice(&self.dscale.to_be_bytes());
    }
}

/// Formats a numeric's binary value the way Postgres's numeric_out does.
pub(crate) fn numeric_to_string(data: &[u8]) -> Result<String, ErrorKind> {
    let header = NumericHeader::decode(data)?;
    match header.sign {
        SIGN_NAN => return Ok("NaN".to_string()),
        SIGN_PINF => return Ok("Infinity".to_string()),
        SIGN_NINF => return Ok("-Infinity".to_string()),
        SIGN_POS | SIGN_NEG => {}
        other => return Err(decode_error(&format!("unknown sign {other:#06x}"))),
    }

    let ndigits = header.ndigits as usize;
    let dscale = header.dscale as usize;
    if data.len() != NumericHeader::LEN + 2 * ndigits {
        return Err(decode_error(&format!(
            "{ndigits} digits do not fit in {} bytes",
            data.len()
        )));
    }
    // as_chunks splits the slice into 2-byte arrays (.0) and the bytes left
    // over (.1). The length check above means nothing is left over.
    let digits: Vec<i16> = data[NumericHeader::LEN..]
        .as_chunks::<2>()
        .0
        .iter()
        .map(|c| i16::from_be_bytes(*c))
        .collect();
    if digits.iter().any(|d| !(0..NBASE).contains(d)) {
        return Err(decode_error("a digit is outside 0 to 9999"));
    }

    let mut out = String::with_capacity(DEC_DIGITS * (ndigits + 2) + dscale + 2);
    if header.sign == SIGN_NEG {
        out.push('-');
    }

    let weight = i64::from(header.weight);
    if weight < 0 {
        out.push('0');
    } else {
        // The integer part, from the digit of weight `weight` down to the
        // digit of weight 0. `(0..=weight).rev()` counts down and includes 0.
        for w in (0..=weight).rev() {
            let group = format!("{:04}", digit_with_weight(&header, &digits, w));
            if w == weight {
                // The first group has no leading zeros, though 0 stays "0".
                let trimmed = group.trim_start_matches('0');
                out.push_str(if trimmed.is_empty() { "0" } else { trimmed });
            } else {
                out.push_str(&group);
            }
        }
    }

    if dscale > 0 {
        out.push('.');
        // Fractional digits have negative weights: -1 for the first four
        // decimal places, -2 for the next four, and so on.
        let mut fraction = String::with_capacity(dscale + DEC_DIGITS);
        let mut w = -1;
        while fraction.len() < dscale {
            fraction.push_str(&format!("{:04}", digit_with_weight(&header, &digits, w)));
            w -= 1;
        }
        out.push_str(&fraction[..dscale]);
    }
    Ok(out)
}

/// Writes a decimal string as a numeric's binary value, without the field's
/// length prefix.
///
/// Accepts what numeric_in accepts: an optional sign, digits with an optional
/// point, an optional exponent, and NaN, Infinity and inf in any case.
pub(crate) fn string_to_numeric(text: &str, out: &mut Vec<u8>) -> Result<(), String> {
    let text = text.trim();
    let special = match text.to_ascii_lowercase().as_str() {
        "nan" => Some(SIGN_NAN),
        "infinity" | "+infinity" | "inf" | "+inf" => Some(SIGN_PINF),
        "-infinity" | "-inf" => Some(SIGN_NINF),
        _ => None,
    };
    if let Some(sign) = special {
        NumericHeader::special(sign).encode(out);
        return Ok(());
    }

    let invalid = || format!("{text:?} is not a valid numeric");
    let (negative, rest) = match text.as_bytes().first() {
        Some(b'-') => (true, &text[1..]),
        Some(b'+') => (false, &text[1..]),
        _ => (false, text),
    };
    let (mantissa, exponent) = match rest.find(['e', 'E']) {
        Some(i) => (
            &rest[..i],
            rest[i + 1..].parse::<i64>().map_err(|_| invalid())?,
        ),
        None => (rest, 0),
    };
    let (int_digits, frac_digits) = match mantissa.find('.') {
        Some(i) => (&mantissa[..i], &mantissa[i + 1..]),
        None => (mantissa, ""),
    };
    let is_digits = |s: &str| s.bytes().all(|b| b.is_ascii_digit());
    if (int_digits.is_empty() && frac_digits.is_empty())
        || !is_digits(int_digits)
        || !is_digits(frac_digits)
    {
        return Err(invalid());
    }

    // The digits as one run, with the decimal point `point` digits in. The
    // exponent moves the point.
    let digits: Vec<u8> = int_digits
        .bytes()
        .chain(frac_digits.bytes())
        .map(|b| b - b'0')
        .collect();
    let point = int_digits.len() as i64 + exponent;
    let dscale = (frac_digits.len() as i64 - exponent).max(0) as usize;
    if dscale > MAX_DSCALE {
        return Err(format!(
            "{text:?} has more than {MAX_DSCALE} digits after the point"
        ));
    }

    // Group the digits in fours, aligned on the decimal point. Digit i has
    // decimal exponent point - 1 - i, so it belongs to the base-10000 digit of
    // weight exponent / 4, at place exponent % 4 within it.
    let mut groups: Vec<(i64, i16)> = vec![];
    for (i, &d) in digits.iter().enumerate() {
        let exponent = point - 1 - i as i64;
        let w = exponent.div_euclid(DEC_DIGITS as i64);
        let place = exponent.rem_euclid(DEC_DIGITS as i64) as u32;
        let value = i16::from(d) * 10i16.pow(place);
        match groups.last_mut() {
            Some((last, sum)) if *last == w => *sum += value,
            _ => groups.push((w, value)),
        }
    }

    // Leading and trailing zero groups are not stored.
    while groups.first().is_some_and(|(_, v)| *v == 0) {
        groups.remove(0);
    }
    while groups.last().is_some_and(|(_, v)| *v == 0) {
        groups.pop();
    }

    let Some(&(weight, _)) = groups.first() else {
        // Zero is positive with no digits.
        let zero = NumericHeader {
            ndigits: 0,
            weight: 0,
            sign: SIGN_POS,
            dscale: dscale as u16,
        };
        zero.encode(out);
        return Ok(());
    };
    if weight > MAX_WEIGHT {
        return Err(format!("{text:?} is too large for a numeric"));
    }
    if weight < i64::from(i16::MIN) {
        return Err(format!("{text:?} is too small for a numeric"));
    }

    // Groups between the first and last are consecutive weights, but a group
    // of zeros in the middle has no digits and so no entry. Fill those in.
    let last_weight = groups.last().unwrap().0;
    let ndigits = (weight - last_weight + 1) as usize;
    let mut stored = vec![0i16; ndigits];
    for (w, v) in groups {
        stored[(weight - w) as usize] = v;
    }

    let header = NumericHeader {
        ndigits: ndigits as u16,
        weight: weight as i16,
        sign: if negative { SIGN_NEG } else { SIGN_POS },
        dscale: dscale as u16,
    };
    header.encode(out);
    for d in stored {
        out.extend_from_slice(&d.to_be_bytes());
    }
    Ok(())
}

/// The base-10000 digit of weight w. The first stored digit has the header's
/// weight and each later one is one less. Digits not stored are 0.
fn digit_with_weight(header: &NumericHeader, digits: &[i16], w: i64) -> i16 {
    let index = i64::from(header.weight) - w;
    if index >= 0 && (index as usize) < digits.len() {
        digits[index as usize]
    } else {
        0
    }
}

fn read_u16(data: &[u8], offset: usize) -> u16 {
    u16::from_be_bytes([data[offset], data[offset + 1]])
}

fn decode_error(reason: &str) -> ErrorKind {
    ErrorKind::Decode {
        reason: format!("invalid numeric: {reason}"),
        name: String::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn round_trip(text: &str) -> String {
        let mut buf = vec![];
        string_to_numeric(text, &mut buf).unwrap();
        numeric_to_string(&buf).unwrap()
    }

    #[test]
    fn test_round_trips_keep_the_display_scale() {
        for text in [
            "0",
            "1",
            "-1",
            "1.50",
            "1.00",
            "0.0001",
            "-0.00012300",
            "12345678.9",
            "10000",
            "100000000",
            "99999999.99999999",
            "0.10",
            "123456789012345678901234567890.5",
            "NaN",
            "Infinity",
            "-Infinity",
            "0.000",
        ] {
            assert_eq!(round_trip(text), text);
        }
    }

    #[test]
    fn test_parsing_normalizes_like_postgres() {
        assert_eq!(round_trip("+1.5"), "1.5");
        assert_eq!(round_trip("007"), "7");
        assert_eq!(round_trip(".5"), "0.5");
        assert_eq!(round_trip("5."), "5");
        assert_eq!(round_trip("1.5e3"), "1500");
        assert_eq!(round_trip("1.5E-3"), "0.0015");
        assert_eq!(round_trip("-0"), "0");
        assert_eq!(round_trip(" 2.5 "), "2.5");
        assert_eq!(round_trip("nan"), "NaN");
        assert_eq!(round_trip("-inf"), "-Infinity");
    }

    #[test]
    fn test_encoding_matches_postgres_bytes() {
        // SELECT numeric_send('1.50'::numeric): 2 digits, weight 0, positive,
        // scale 2, digits 1 and 5000.
        let mut buf = vec![];
        string_to_numeric("1.50", &mut buf).unwrap();
        assert_eq!(buf, [0, 2, 0, 0, 0, 0, 0, 2, 0, 1, 0x13, 0x88]);

        // SELECT numeric_send('-20000.5'::numeric): weight 1, digits 2, 0, 5000.
        let mut buf = vec![];
        string_to_numeric("-20000.5", &mut buf).unwrap();
        assert_eq!(buf, [0, 3, 0, 1, 0x40, 0, 0, 1, 0, 2, 0, 0, 0x13, 0x88]);
    }

    #[test]
    fn test_invalid_strings_are_rejected() {
        for text in ["", "-", ".", "1.2.3", "abc", "1e", "1e5x", "--1", "1,5"] {
            assert!(string_to_numeric(text, &mut vec![]).is_err(), "{text:?}");
        }
    }

    #[test]
    fn test_malformed_binary_is_rejected() {
        assert!(numeric_to_string(&[0, 1]).is_err());
        // One digit announced, none present.
        assert!(numeric_to_string(&[0, 1, 0, 0, 0, 0, 0, 0]).is_err());
        // A digit of 10000.
        assert!(numeric_to_string(&[0, 1, 0, 0, 0, 0, 0, 0, 0x27, 0x10]).is_err());
    }
}
