//! IANA special-purpose IP ranges, and the DNS names that never denote a
//! public host: the destinations a request Brenn sends on someone else's
//! behalf must never reach.
//!
//! IPv4-mapped IPv6 addresses (`::ffff:a.b.c.d`) are judged as the IPv4
//! address they carry. Teredo (`2001::/32`) and 6to4 (`2002::/16`) are special
//! outright because they tunnel arbitrary IPv4 addresses inside IPv6.

use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

/// Whether `addr` lies in any IANA special-purpose range (RFC 6890 and the
/// registries it umbrellas): loopback, private, link-local, shared, multicast,
/// documentation, benchmarking, reserved, and the IPv6 translation and
/// tunnelling prefixes.
pub fn is_special_purpose(addr: IpAddr) -> bool {
    match addr {
        IpAddr::V4(v4) => special_ipv4(v4),
        IpAddr::V6(v6) => match v6.to_ipv4_mapped() {
            Some(v4) => special_ipv4(v4),
            None => special_ipv6(v6),
        },
    }
}

/// Name suffixes under which no public host lives: RFC 6761's `localhost`,
/// RFC 6762's `local`, RFC 8375's `home.arpa`, `internal` (reserved for
/// private use), and the private suffixes networks commonly configure.
const SPECIAL_USE_SUFFIXES: &[&str] = &[
    "localhost",
    "local",
    "home.arpa",
    "internal",
    "lan",
    "corp",
    "intranet",
    "private",
    "localdomain",
];

/// Whether the DNS name `name` never denotes a public host: a single label
/// (which resolves, if at all, through local configuration), or a name equal
/// to or under one of [`SPECIAL_USE_SUFFIXES`]. Trailing dots are ignored and
/// labels compared without regard to ASCII case; `name` is a URL host, which
/// the WHATWG parser has already made ASCII.
pub fn is_special_use_name(name: &str) -> bool {
    let name = name.trim_end_matches('.');
    if !name.contains('.') {
        return true;
    }
    SPECIAL_USE_SUFFIXES.iter().any(|suffix| {
        name.eq_ignore_ascii_case(suffix)
            || (name.len() > suffix.len()
                && name[name.len() - suffix.len()..].eq_ignore_ascii_case(suffix)
                && name.as_bytes()[name.len() - suffix.len() - 1] == b'.')
    })
}

/// Uses both the explicit CIDR table (for ranges not yet stabilised as
/// `std::net::Ipv4Addr` predicates) and every stable std predicate.
fn special_ipv4(addr: Ipv4Addr) -> bool {
    if addr.is_unspecified()
        || addr.is_loopback()
        || addr.is_private()
        || addr.is_link_local()
        || addr.is_multicast()
        || addr.is_broadcast()
        || addr.is_documentation()
    {
        return true;
    }

    // Ranges already covered by the std predicates above (loopback 127/8,
    // private 10/8 + 172.16/12 + 192.168/16, link-local 169.254/16, multicast
    // 224/4, broadcast, documentation 192.0.2/24 + 198.51.100/24 +
    // 203.0.113/24) are omitted here.
    static IPV4_CIDR_TABLE: &[(u32, u32)] = &[
        // (mask, network_bits) — special if `addr_u32 & mask == network_bits`.
        // "this network": 0.0.0.0/8 (RFC 1122); is_unspecified() covers only
        // 0.0.0.0/32.
        (0xFF00_0000, 0x0000_0000),
        // CGNAT shared address space: 100.64.0.0/10
        (0xFFC0_0000, 0x6440_0000),
        // IETF protocol assignments: 192.0.0.0/24
        (0xFFFF_FF00, 0xC000_0000),
        // Benchmarking: 198.18.0.0/15
        (0xFFFE_0000, 0xC612_0000),
        // Reserved (class E): 240.0.0.0/4
        (0xF000_0000, 0xF000_0000),
    ];

    let addr_u32 = u32::from(addr);
    IPV4_CIDR_TABLE
        .iter()
        .any(|&(mask, net)| addr_u32 & mask == net)
}

/// Never called with an IPv4-mapped address: [`is_special_purpose`] unwraps
/// those first.
fn special_ipv6(addr: Ipv6Addr) -> bool {
    if addr.is_unspecified() || addr.is_loopback() || addr.is_multicast() {
        return true;
    }

    // Each entry: (128-bit prefix, prefix length in bits). Ranges covered by
    // the std predicates above (::/128, ::1/128, ff00::/8) are omitted.
    static IPV6_CIDR_TABLE: &[(u128, u32)] = &[
        // NAT64 translation: 64:ff9b::/96
        (0x0064_ff9b_0000_0000_0000_0000_0000_0000u128, 96),
        // NAT64 translation local-use: 64:ff9b:1::/48
        (0x0064_ff9b_0001_0000_0000_0000_0000_0000u128, 48),
        // Discard-only: 100::/64
        (0x0100_0000_0000_0000_0000_0000_0000_0000u128, 64),
        // IETF protocol assignments: 2001::/23 covers 2001:0000:: – 2001:01ff::,
        // Teredo (2001::/32) and benchmarking (2001:2::/48) among them.
        (0x2001_0000_0000_0000_0000_0000_0000_0000u128, 23),
        // Documentation: 2001:db8::/32 (not inside 2001::/23)
        (0x2001_0db8_0000_0000_0000_0000_0000_0000u128, 32),
        // 6to4: 2002::/16
        (0x2002_0000_0000_0000_0000_0000_0000_0000u128, 16),
        // Documentation: 3fff::/20
        (0x3fff_0000_0000_0000_0000_0000_0000_0000u128, 20),
        // Unique-local: fc00::/7
        (0xfc00_0000_0000_0000_0000_0000_0000_0000u128, 7),
        // Link-local unicast: fe80::/10
        (0xfe80_0000_0000_0000_0000_0000_0000_0000u128, 10),
    ];

    let addr_u128 = u128::from(addr);
    IPV6_CIDR_TABLE.iter().any(|&(prefix, prefix_len)| {
        let shift = 128u32.saturating_sub(prefix_len);
        addr_u128 >> shift == prefix >> shift
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn special(s: &str) -> bool {
        is_special_purpose(s.parse().unwrap())
    }

    #[test]
    fn every_ipv4_special_range_is_special() {
        for addr in [
            "0.0.0.0",
            "0.1.2.3",
            "127.0.0.1",
            "10.1.2.3",
            "172.16.0.1",
            "172.31.255.255",
            "192.168.1.1",
            "169.254.169.254",
            "100.64.0.1",
            "100.127.255.255",
            "192.0.0.8",
            "192.0.2.1",
            "198.51.100.1",
            "203.0.113.1",
            "198.18.0.1",
            "198.19.255.255",
            "224.0.0.1",
            "239.255.255.255",
            "240.0.0.1",
            "255.255.255.255",
        ] {
            assert!(special(addr), "{addr}");
        }
    }

    #[test]
    fn public_ipv4_is_not_special() {
        for addr in [
            "1.1.1.1",
            "8.8.8.8",
            "93.184.216.34",
            "100.63.255.255",
            "100.128.0.0",
            "172.15.255.255",
            "172.32.0.0",
            "192.0.1.1",
            "198.17.255.255",
            "198.20.0.0",
            "223.255.255.255",
        ] {
            assert!(!special(addr), "{addr}");
        }
    }

    #[test]
    fn every_ipv6_special_range_is_special() {
        for addr in [
            "::",
            "::1",
            "64:ff9b::a00:1",
            "64:ff9b:1::1",
            "100::1",
            "2001::1",
            "2001:2::1",
            "2001:1ff::1",
            "2001:db8::1",
            "2002:7f00:1::",
            "3fff::1",
            "fc00::1",
            "fd12:3456::1",
            "fe80::1",
            "ff02::1",
        ] {
            assert!(special(addr), "{addr}");
        }
    }

    #[test]
    fn public_ipv6_is_not_special() {
        for addr in ["2606:4700:4700::1111", "2001:4860:4860::8888", "2a00::1"] {
            assert!(!special(addr), "{addr}");
        }
    }

    #[test]
    fn ipv4_mapped_is_judged_as_its_ipv4() {
        assert!(special("::ffff:127.0.0.1"));
        assert!(special("::ffff:10.0.0.1"));
        assert!(special("::ffff:169.254.169.254"));
        assert!(!special("::ffff:8.8.8.8"));
    }

    #[test]
    fn special_use_names_are_special() {
        for name in [
            "localhost",
            "LOCALHOST",
            "localhost.",
            "localhost..",
            "app.localhost",
            "printer.local",
            "printer.local.",
            "nas.home.arpa",
            "home.arpa",
            "db.internal",
            "router.lan",
            "mail.corp",
            "wiki.intranet",
            "box.private",
            "host.localdomain",
            "intranet",
            "ip6-localhost",
            "",
            ".",
        ] {
            assert!(is_special_use_name(name), "{name:?}");
        }
    }

    #[test]
    fn public_names_are_not_special_use() {
        for name in [
            "example.com",
            "lists.example.com.",
            "local.example.com",
            "mylocal.example",
            "lan.example.org",
            "arpa.example",
            "examplelocal.com",
            "evil-localhost.com",
        ] {
            assert!(!is_special_use_name(name), "{name:?}");
        }
    }
}
