//! The outbound-URL guard (SSRF), shared by `url` jobs and the api / datatable source fetch.
//!
//! - [`validate_url`]: an http(s) URL without userinfo whose host is not internal (dotless,
//!   `.internal`, `.local`, `.svc`, …) and not a literal IP outside the public internet. This is
//!   all a `url` job needs at submit time: the webscraper resolves and fetches it.
//! - [`resolve_public`]: resolves a host and refuses it when ANY address is not public, so a
//!   public name pointing at a private address (DNS rebinding included) is refused. The caller
//!   then pins the connection to exactly these addresses (`sources::fetch`).
//! - Tenant-registered provider `base_url`s (`PUT /api/tenant/providers`, same rules as
//!   strata_rag's guard): [`parse_provider_url`] (canonical form only, so the host checked is the
//!   host fetched), [`check_provider_host`] (https, no internal name or non-public IP literal, no
//!   numeric host spellings), [`check_provider_registration`] (every resolved address public) and
//!   [`GuardedResolver`] (tenant clients never connect to a non-public address, also after a DNS
//!   change; redirects and proxies are off in those clients).

use std::collections::HashMap;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};

use crate::error::{IngestError, Result};

/// Longest accepted URL.
pub const MAX_URL_LEN: usize = 2048;

fn bad(msg: impl Into<String>) -> IngestError {
    IngestError::BadRequest(msg.into())
}

/// Hostname suffixes that only name hosts inside a private network.
const INTERNAL_SUFFIXES: [&str; 7] = [
    ".internal",
    ".local",
    ".localhost",
    ".svc",
    ".cluster.local",
    ".lan",
    ".home.arpa",
];

/// True for a hostname that cannot be a public internet name: dotless (`minio`, `localhost`) or
/// under an internal suffix. Case-insensitive; trailing dots ignored.
pub fn internal_hostname(host: &str) -> bool {
    let h = host.trim_end_matches('.').to_ascii_lowercase();
    !h.contains('.')
        || INTERNAL_SUFFIXES
            .iter()
            .any(|s| h.ends_with(s) || h == s[1..])
}

/// An http(s) URL without credentials whose host is not internal and not a literal IP outside
/// the public internet. Returns the parsed URL. Hostnames are not resolved here.
pub fn check_url(raw: &str) -> Result<url::Url> {
    if raw.len() > MAX_URL_LEN {
        return Err(bad(format!("URL longer than {MAX_URL_LEN} bytes")));
    }
    let url = url::Url::parse(raw.trim()).map_err(|e| bad(format!("invalid URL: {e}")))?;
    if !matches!(url.scheme(), "http" | "https") {
        return Err(bad("only http and https URLs are accepted"));
    }
    if !url.username().is_empty() || url.password().is_some() {
        return Err(bad("URLs with credentials are not accepted"));
    }
    let blocked = match url.host() {
        None => return Err(bad("URL has no host")),
        Some(url::Host::Domain(d)) => internal_hostname(d),
        Some(url::Host::Ipv4(ip)) => !public_v4(ip),
        Some(url::Host::Ipv6(ip)) => !public_v6(ip),
    };
    if blocked {
        return Err(bad("URL host is a private, loopback or reserved address"));
    }
    Ok(url)
}

/// [`check_url`], normalised to a string.
pub fn validate_url(raw: &str) -> Result<String> {
    check_url(raw).map(|u| u.to_string())
}

/// An address on the public internet (not private, loopback, link-local, CGNAT, multicast,
/// documentation, reserved, or a v6 form embedding such a v4 address).
pub fn public_ip(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(v4) => public_v4(v4),
        IpAddr::V6(v6) => public_v6(v6),
    }
}

pub fn public_v4(ip: Ipv4Addr) -> bool {
    let [a, b, c, _] = ip.octets();
    !(ip.is_unspecified()
        || ip.is_loopback()
        || ip.is_private()
        || ip.is_link_local()
        || ip.is_broadcast()
        || ip.is_documentation()
        || ip.is_multicast()
        || a == 0
        || (a == 100 && (64..128).contains(&b)) // CGNAT 100.64/10
        || (a == 192 && b == 0 && c == 0) // IETF protocol assignments
        || (a == 198 && (b == 18 || b == 19)) // benchmarking
        || (a == 192 && b == 88 && c == 99) // 6to4 relay anycast
        || a >= 240) // reserved
}

fn v4_at(s: &[u16], i: usize) -> Ipv4Addr {
    let [a, b] = s[i].to_be_bytes();
    let [c, d] = s[i + 1].to_be_bytes();
    Ipv4Addr::new(a, b, c, d)
}

pub fn public_v6(ip: Ipv6Addr) -> bool {
    if let Some(v4) = ip.to_ipv4_mapped() {
        return public_v4(v4);
    }
    let s = ip.segments();
    // NAT64 well-known 64:ff9b::/96 embeds an IPv4 address in the last 32 bits.
    if s[0] == 0x64 && s[1] == 0xff9b && s[2..6] == [0, 0, 0, 0] {
        return public_v4(v4_at(&s, 6));
    }
    // NAT64 local-use 64:ff9b:1::/48 (RFC 8215): the embedding position depends on the local
    // prefix length, so the whole range is refused.
    if s[0] == 0x64 && s[1] == 0xff9b && s[2] == 1 {
        return false;
    }
    // 6to4 2002::/16: the IPv4 address is bits 16..48.
    if s[0] == 0x2002 {
        return public_v4(v4_at(&s, 1));
    }
    // Teredo 2001:0::/32: the client IPv4 is the bitwise NOT of the last 32 bits.
    if s[0] == 0x2001 && s[1] == 0 {
        let v4 = u32::from(v4_at(&s, 6));
        return public_v4(Ipv4Addr::from(!v4));
    }
    // IPv4-translated ::ffff:0:a.b.c.d (RFC 2765).
    if s[..6] == [0, 0, 0, 0, 0xffff, 0] {
        return public_v4(v4_at(&s, 6));
    }
    !(ip.is_unspecified()
        || ip.is_loopback()
        || ip.is_multicast()
        || (s[0] & 0xfe00) == 0xfc00 // unique local
        || (s[0] & 0xffc0) == 0xfe80 // link local
        || (s[0] & 0xffc0) == 0xfec0 // site local (deprecated)
        || (s[0] == 0x2001 && s[1] == 0x0db8) // documentation
        || s[..4] == [0x100, 0, 0, 0] // discard-only 100::/64
        || s[..6] == [0, 0, 0, 0, 0, 0]) // IPv4-compatible (deprecated) and the like
}

/// Where a host resolves for an outbound fetch. Production uses the default: the system resolver
/// and only public addresses. Tests pin names to addresses and allow their own listeners.
#[derive(Debug, Clone, Default)]
pub struct Resolution {
    /// Fixed answers by lower-case hostname, used instead of DNS.
    pub hosts: HashMap<String, Vec<IpAddr>>,
    /// Exact socket addresses allowed although not public (a test's own listener).
    pub allow: Vec<SocketAddr>,
}

/// Why a host was refused or could not be resolved.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ResolveError {
    /// The name resolves to (at least one) non-public address.
    Blocked,
    /// No address (NXDOMAIN, resolver failure, empty answer).
    Unresolvable(String),
}

/// Resolves `url`'s host to the socket addresses to connect to, all of them public (or in
/// [`Resolution::allow`]). A literal IP is checked as is. One non-public address refuses the
/// whole name: a mixed answer is how DNS rebinding gets in.
pub async fn resolve_public(
    url: &url::Url,
    res: &Resolution,
) -> std::result::Result<Vec<SocketAddr>, ResolveError> {
    let port = url
        .port_or_known_default()
        .ok_or_else(|| ResolveError::Unresolvable("URL has no port".into()))?;
    let addrs: Vec<SocketAddr> = match url.host() {
        None => return Err(ResolveError::Unresolvable("URL has no host".into())),
        Some(url::Host::Ipv4(ip)) => vec![SocketAddr::new(IpAddr::V4(ip), port)],
        Some(url::Host::Ipv6(ip)) => vec![SocketAddr::new(IpAddr::V6(ip), port)],
        Some(url::Host::Domain(d)) => {
            let host = d.trim_end_matches('.').to_ascii_lowercase();
            match res.hosts.get(&host) {
                Some(ips) => ips.iter().map(|ip| SocketAddr::new(*ip, port)).collect(),
                None => tokio::net::lookup_host((host.as_str(), port))
                    .await
                    .map_err(|e| ResolveError::Unresolvable(e.to_string()))?
                    .collect(),
            }
        }
    };
    if addrs.is_empty() {
        return Err(ResolveError::Unresolvable("no address".into()));
    }
    if addrs
        .iter()
        .any(|a| !public_ip(a.ip()) && !res.allow.contains(a))
    {
        return Err(ResolveError::Blocked);
    }
    Ok(addrs)
}

// ---------------------------------------------------------------------------------------------
// Tenant-registered provider URLs

/// Why a provider `base_url` was refused. [`code`](Self::code) is the 422 `error.code`; the
/// message names the rule, never a resolved address or a resolver error.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProviderUrlError {
    /// Backslash, whitespace or a control character in the raw input (the WHATWG parser would
    /// silently strip or reinterpret them, so what is checked would differ from what is fetched).
    Unsafe,
    NotAUrl,
    Scheme,
    Credentials,
    QueryOrFragment,
    NoHost,
    /// `url.as_str()` differs from the input (case, IDNA, percent-encoding, default port, path
    /// normalisation, a numeric IPv4 spelling): only the canonical form is accepted.
    NotCanonical,
    NotHttps,
    NonPublicAddress,
    InternalName,
    NumericSpelling,
    Unresolvable,
    ResolvesNonPublic,
}

pub const PROVIDER_HOST_NOT_ALLOWED: &str = "provider_host_not_allowed";

impl ProviderUrlError {
    pub fn code(self) -> &'static str {
        match self {
            Self::NotAUrl | Self::Scheme | Self::QueryOrFragment => "invalid_provider",
            _ => PROVIDER_HOST_NOT_ALLOWED,
        }
    }

    pub fn message(self) -> &'static str {
        match self {
            Self::Unsafe => "must not contain backslashes, whitespace or control characters",
            Self::NotAUrl => "is not a valid URL",
            Self::Scheme => "must start with http:// or https://",
            Self::Credentials => "must not carry credentials (send the key as api_key)",
            Self::QueryOrFragment => "must not have a query or fragment",
            Self::NoHost => "needs a host",
            Self::NotCanonical => {
                "must be in canonical form (lowercase ASCII host, no default port, no \
                 percent-encoded host, normalised path)"
            }
            Self::NotHttps => "must use https",
            Self::NonPublicAddress => "is a non-public address",
            Self::InternalName => "is an internal host name",
            Self::NumericSpelling => "host must be a DNS name or a dotted public IP",
            Self::Unresolvable => "host does not resolve",
            Self::ResolvesNonPublic => "resolves to a non-public address",
        }
    }
}

/// Parses `raw` with `url::Url` (reqwest's parser) and accepts it only when nothing about it is
/// ambiguous: no backslash / whitespace / control characters, http(s), no userinfo, no query or
/// fragment, a host, and `url.as_str()` equal to the input (up to a trailing `/`). The stored
/// `base_url` is therefore exactly what reqwest fetches. Never normalises.
pub fn parse_provider_url(raw: &str) -> std::result::Result<url::Url, ProviderUrlError> {
    if raw
        .chars()
        .any(|c| c == '\\' || c.is_whitespace() || c.is_control())
    {
        return Err(ProviderUrlError::Unsafe);
    }
    let url = url::Url::parse(raw).map_err(|_| ProviderUrlError::NotAUrl)?;
    if !matches!(url.scheme(), "http" | "https") {
        return Err(ProviderUrlError::Scheme);
    }
    if !url.username().is_empty() || url.password().is_some() {
        return Err(ProviderUrlError::Credentials);
    }
    if url.query().is_some() || url.fragment().is_some() {
        return Err(ProviderUrlError::QueryOrFragment);
    }
    if url.host().is_none() {
        return Err(ProviderUrlError::NoHost);
    }
    if url.as_str().trim_end_matches('/') != raw.trim_end_matches('/') {
        return Err(ProviderUrlError::NotCanonical);
    }
    Ok(url)
}

/// The static checks (no DNS) on the host reqwest will connect to: [`parse_provider_url`], https,
/// an IP literal only when public, a domain only when not internal ([`internal_hostname`]) and
/// not a numeric IPv4 spelling (decimal / hex labels). Returns that host.
pub fn check_provider_host(raw: &str) -> std::result::Result<url::Host<String>, ProviderUrlError> {
    let url = parse_provider_url(raw)?;
    if url.scheme() != "https" {
        return Err(ProviderUrlError::NotHttps);
    }
    let host = url
        .host()
        .map(|h| h.to_owned())
        .ok_or(ProviderUrlError::NoHost)?;
    match &host {
        url::Host::Ipv4(ip) if !public_v4(*ip) => Err(ProviderUrlError::NonPublicAddress),
        url::Host::Ipv6(ip) if !provider_ip_allowed(IpAddr::V6(*ip)) => {
            Err(ProviderUrlError::NonPublicAddress)
        }
        url::Host::Ipv4(_) | url::Host::Ipv6(_) => Ok(host),
        url::Host::Domain(d) => {
            let d = d.trim_end_matches('.').to_ascii_lowercase();
            if d.chars().all(|c| c.is_ascii_digit() || c == '.')
                || d.split('.').any(|label| {
                    label.strip_prefix("0x").is_some_and(|hex| {
                        !hex.is_empty() && hex.chars().all(|c| c.is_ascii_hexdigit())
                    })
                })
            {
                return Err(ProviderUrlError::NumericSpelling);
            }
            if internal_hostname(&d) {
                return Err(ProviderUrlError::InternalName);
            }
            Ok(host)
        }
    }
}

/// The registration-time check: [`check_provider_host`], then every address the host resolves to
/// (5 s at most) must be public. A host that does not resolve is refused (it could not be probed).
pub async fn check_provider_registration(raw: &str) -> std::result::Result<(), ProviderUrlError> {
    let url::Host::Domain(host) = check_provider_host(raw)? else {
        return Ok(());
    };
    let addrs: Vec<SocketAddr> = match tokio::time::timeout(
        std::time::Duration::from_secs(5),
        tokio::net::lookup_host((host.as_str(), 443)),
    )
    .await
    {
        Ok(Ok(a)) => a.collect(),
        _ => return Err(ProviderUrlError::Unresolvable),
    };
    if addrs.is_empty() {
        return Err(ProviderUrlError::Unresolvable);
    }
    if addrs.iter().any(|a| !provider_ip_allowed(a.ip())) {
        return Err(ProviderUrlError::ResolvesNonPublic);
    }
    Ok(())
}

/// An address a tenant provider may be reached at: [`public_ip`], and not Teredo (2001::/32, a
/// tunnel; refused whole for providers, as strata_rag does — `url` jobs keep the finer check).
pub fn provider_ip_allowed(ip: IpAddr) -> bool {
    if let IpAddr::V6(v6) = ip {
        let s = v6.segments();
        if s[0] == 0x2001 && s[1] == 0 {
            return false;
        }
    }
    public_ip(ip)
}

/// `addrs` without the ones a provider may not be reached at (the connect-time filter).
pub fn public_only(addrs: impl IntoIterator<Item = SocketAddr>) -> Vec<SocketAddr> {
    addrs
        .into_iter()
        .filter(|a| provider_ip_allowed(a.ip()))
        .collect()
}

/// reqwest resolver for tenant provider clients: the system resolver minus every non-public
/// address. Nothing public left (or nothing resolved) is a connect error with a fixed message.
#[derive(Debug, Default, Clone, Copy)]
pub struct GuardedResolver;

#[derive(Debug)]
struct NotAllowed;

impl std::fmt::Display for NotAllowed {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("provider host not allowed")
    }
}

impl std::error::Error for NotAllowed {}

impl reqwest::dns::Resolve for GuardedResolver {
    fn resolve(&self, name: reqwest::dns::Name) -> reqwest::dns::Resolving {
        let host = name.as_str().to_string();
        Box::pin(async move {
            let refused = || Box::new(NotAllowed) as Box<dyn std::error::Error + Send + Sync>;
            let addrs = tokio::net::lookup_host((host.as_str(), 0))
                .await
                .map_err(|_| refused())?;
            let ok = public_only(addrs);
            if ok.is_empty() {
                return Err(refused());
            }
            Ok(Box::new(ok.into_iter()) as reqwest::dns::Addrs)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn literal_and_internal_hosts_are_refused() {
        for u in [
            "http://127.0.0.1/x",
            "http://10.1.2.3/",
            "http://100.64.0.2/",
            "http://169.254.169.254/latest",
            "http://[::1]/",
            "http://[::ffff:10.0.0.1]/",
            "http://localhost/",
            "http://db.example.internal/",
            "http://storage-api.example.svc.cluster.local:8391/",
            "ftp://example.com/",
            "http://user:pw@example.com/",
        ] {
            assert!(validate_url(u).is_err(), "{u}");
        }
        assert!(validate_url("https://api.example.com/v1/items?x=1").is_ok());
    }

    fn res(host: &str, ips: &[&str]) -> Resolution {
        Resolution {
            hosts: HashMap::from([(
                host.to_string(),
                ips.iter().map(|i| i.parse().unwrap()).collect(),
            )]),
            allow: vec![],
        }
    }

    #[tokio::test]
    async fn a_public_name_resolving_to_a_private_address_is_blocked() {
        let url = url::Url::parse("https://api.example.com/items").unwrap();
        for ips in [
            &["10.0.0.5"][..],
            &["127.0.0.1"],
            &["100.64.1.1"],
            &["169.254.169.254"],
            &["fd00::1"],
            // One private address among public ones refuses the name (rebinding).
            &["93.184.216.34", "192.168.1.10"],
        ] {
            let r = resolve_public(&url, &res("api.example.com", ips)).await;
            assert_eq!(r, Err(ResolveError::Blocked), "{ips:?}");
        }
        let ok = resolve_public(&url, &res("api.example.com", &["93.184.216.34"]))
            .await
            .unwrap();
        assert_eq!(ok, vec!["93.184.216.34:443".parse().unwrap()]);
    }

    #[tokio::test]
    async fn only_the_exact_allowed_socket_passes() {
        let url = url::Url::parse("http://src.example.test:4000/").unwrap();
        let mut r = res("src.example.test", &["127.0.0.1"]);
        r.allow = vec!["127.0.0.1:4000".parse().unwrap()];
        assert!(resolve_public(&url, &r).await.is_ok());
        let other = url::Url::parse("http://src.example.test:4001/").unwrap();
        assert_eq!(resolve_public(&other, &r).await, Err(ResolveError::Blocked));
    }

    fn blocked(ip: &str) -> bool {
        !provider_ip_allowed(ip.parse().unwrap())
    }

    #[test]
    fn provider_guard_blocks_every_range_strata_rag_blocks() {
        for ip in [
            "127.0.0.1",
            "10.43.0.10",
            "172.31.255.255",
            "192.168.1.1",
            "169.254.169.254",
            "100.64.0.1",
            "100.64.0.2",
            "0.0.0.0",
            "224.0.0.1",
            "255.255.255.255",
            "192.0.0.8",
            "198.18.0.1",
            "192.0.2.1",
            "198.51.100.7",
            "203.0.113.9",
            "192.88.99.1",
            "::1",
            "::",
            "fe80::1",
            "fc00::1",
            "fec0::1",
            "ff02::1",
            "::ffff:10.0.0.1",
            "64:ff9b::a9fe:a9fe",
            "64:ff9b:1::101:101",
            "::ffff:0:a00:1",
            "2001:db8::1",
            "2001::1",
            "2002:0a00:0001::1",
            "100::1",
            "100::ffff:ffff:ffff:ffff",
        ] {
            assert!(blocked(ip), "{ip} must be blocked");
        }
        for ip in [
            "1.1.1.1",
            "100.63.255.255",
            "100.128.0.1",
            "192.88.100.1",
            "198.20.0.1",
            "2606:4700::1111",
            "::ffff:1.1.1.1",
            "::ffff:0:101:101",
            "2002:0101:0101::1",
            "100:0:0:1::1",
        ] {
            assert!(!blocked(ip), "{ip} is public");
        }
    }

    #[test]
    fn provider_host_static_checks() {
        let code = |u: &str| check_provider_host(u).err().map(|e| e.code());
        for bad in [
            "http://api.example.com/v1",
            "https://127.0.0.1:6432/v1",
            "https://[::1]/v1",
            "https://169.254.169.254/latest",
            "https://100.64.0.2:5432",
            "https://10.43.0.1/v1",
            "https://localhost:9131/v1",
            "https://db.example.svc.cluster.local:6432",
            "https://provider.example.internal:9131/v1",
            "https://printer.local/v1",
            "https://pgbouncer/v1",
            "https://2130706433/v1",
            "https://0x7f000001/v1",
            "https://0xff.example.com/v1",
            "https://API.Example.INTERNAL./v1",
            "https://foo.internal./v1",
            "https://user:pw@public.example.com/v1",
            "https://evil@10.0.0.1/",
            "https://10.0.0.1\\.x.attacker.com/v1",
            "https://public.example.com\\@10.0.0.1/v1",
            "https://10.0.0.1\t.public.example.com/v1",
            " https://public.example.com/v1",
            "HTTPS://PUBLIC.EXAMPLE.COM/v1",
            "https://bücher.example/v1",
            "https://[::ffff:a00:1]/",
            "https://%31%30.0.0.1/v1",
            "https://public.example.com:443/v1",
            "https://public.example.com/v1/../admin",
            "https://10.1/v1",
        ] {
            assert_eq!(code(bad), Some(PROVIDER_HOST_NOT_ALLOWED), "{bad:?}");
        }
        for invalid in [
            "ftp://x.example.com",
            "not a url",
            "https://public.example.com/v1?x=1",
            "https://public.example.com/v1#frag",
        ] {
            assert_ne!(code(invalid), None, "{invalid:?}");
        }
        assert_eq!(code("ftp://x.example.com"), Some("invalid_provider"));
        assert_eq!(
            code("https://public.example.com/v1?x=1"),
            Some("invalid_provider")
        );
        for ok in [
            "https://api.mistral.ai/v1",
            "https://xn--bcher-kva.example/v1",
            "https://1.1.1.1/v1",
            "https://[2606:4700::1111]/v2",
            "https://api.example.com:8443/v1",
            "https://0xfoo.example.com/v1",
            "https://cafe.be/v1",
        ] {
            assert_eq!(code(ok), None, "{ok}");
        }
    }

    /// The host the guard checked is the host reqwest connects to.
    #[test]
    fn the_provider_guard_checks_the_host_reqwest_connects_to() {
        for raw in [
            "https://public.example.com/v1",
            "https://1.1.1.1/v1",
            "https://[2606:4700::1111]/v2",
            "https://public.example.com./v1",
        ] {
            let host = check_provider_host(raw).expect(raw);
            let u = reqwest::Url::parse(&format!("{raw}/ocr")).unwrap();
            assert_eq!(u.host().map(|h| h.to_owned()), Some(host), "{raw}");
        }
    }

    #[test]
    fn the_connect_time_filter_drops_internal_addresses() {
        let addrs: Vec<SocketAddr> = [
            "10.0.0.5:443",
            "1.1.1.1:443",
            "[fd00::1]:443",
            "100.100.1.1:443",
            "[2606:4700::1111]:443",
            "169.254.169.254:80",
        ]
        .iter()
        .map(|s| s.parse().unwrap())
        .collect();
        let kept: Vec<String> = public_only(addrs).iter().map(|a| a.to_string()).collect();
        assert_eq!(kept, ["1.1.1.1:443", "[2606:4700::1111]:443"]);
    }

    #[tokio::test]
    async fn the_guarded_resolver_refuses_a_name_that_resolves_only_internally() {
        use reqwest::dns::Resolve;
        let name: reqwest::dns::Name = "localhost".parse().unwrap();
        let err = match GuardedResolver.resolve(name).await {
            Ok(addrs) => panic!("resolved to {:?}", addrs.collect::<Vec<_>>()),
            Err(e) => e.to_string(),
        };
        assert_eq!(err, "provider host not allowed");
    }

    #[tokio::test]
    async fn registration_check_needs_no_dns_for_a_public_literal() {
        assert!(
            check_provider_registration("https://1.1.1.1/v1")
                .await
                .is_ok()
        );
        assert_eq!(
            check_provider_registration("https://localhost/v1").await,
            Err(ProviderUrlError::InternalName)
        );
    }
}
