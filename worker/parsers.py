import re

# ---- Device Type Classification Helpers ----

def classify_from_cdp_caps(caps_text: str) -> str:
    """Map raw CDP/LLDP capability text (long words or single letters) to device_type.

    Handles examples:
      'Router Source-Route-Bridge'
      'Switch IGMP'
      'B,R' (letters)
      'B,R' with parentheses removed
    Letter mapping (Cisco doc):
      R=router, B=bridge (switch), S=switch, H=host, W=wireless AP, P=repeater
    """
    if not caps_text:
        return "unknown"
    txt = caps_text.replace('(', ' ').replace(')', ' ').replace('/', ' ')
    # Split by non-word boundaries except keep single letters
    tokens = re.split(r"[\s,]+", txt.strip())
    norm = set()
    for raw in tokens:
        t = raw.strip().lower()
        if not t:
            continue
        # Single-letter codes
        if len(t) == 1:
            if t == 'r': norm.add('router')
            elif t in ('b','s'): norm.add('switch')
            elif t == 'h': norm.add('end')
            elif t == 'w': norm.add('ap')
            elif t == 'p': continue  # repeater ignore
            continue
        # Word forms
        if t.startswith('router'): norm.add('router')
        elif t.startswith('switch'): norm.add('switch')
        elif t == 'bridge': norm.add('switch')  # Only standalone "bridge", not "source-route-bridge"
        elif t.startswith('host') or t.startswith('station'): norm.add('end')
        elif t.startswith('wlan') or t.startswith('wireless'): norm.add('ap')
    # Decision priority
    if 'router' in norm and 'switch' in norm:
        return 'layer3_switch'
    if 'router' in norm:
        return 'router'
    if 'switch' in norm:
        return 'switch'
    if 'ap' in norm:
        return 'ap'
    if 'end' in norm:
        return 'end'
    return 'unknown'

def classify_from_lldp_caps(sys_caps: str, enabled_caps: str) -> str:
    """Very lightweight LLDP classification (both strings optional)."""
    caps = (sys_caps or "") + " " + (enabled_caps or "")
    return classify_from_cdp_caps(caps)  # reuse logic

_IF_PREFIX_MAP = [
    (re.compile(r"^(GigabitEthernet|GigEthernet|GigEth|Gi)(?=\d)", re.I), "Gi"),
    (re.compile(r"^(TenGigabitEthernet|TenGigE|Te)(?=\d)", re.I), "Te"),
    (re.compile(r"^(FastEthernet|FastEth|Fa)(?=\d)", re.I), "Fa"),
    (re.compile(r"^(Ethernet|Eth|Et)(?=\d)", re.I), "Et"),
    (re.compile(r"^(Port-channel|Port-Channel|Po)(?=\d)", re.I), "Po"),
    (re.compile(r"^(Loopback|Lo)(?=\d)", re.I), "Lo"),
    (re.compile(r"^(Vlan|Vl)(?=\d)", re.I), "Vl"),
]

def normalize_if_name(name: str) -> str:
    """Normalize varied Cisco-like interface strings to short form.

    Examples:
      GigabitEthernet0/1 -> Gi0/1
      Gi0/1            -> Gi0/1 (unchanged)
      FastEthernet0/1  -> Fa0/1
      TenGigabitEthernet1/0/1 -> Te1/0/1
      Port-channel10    -> Po10
    Keeps suffix (/... or .sub) intact. Non-matching names returned unchanged.
    """
    if not name:
        return name
    name = name.strip()
    for pattern, short in _IF_PREFIX_MAP:
        if pattern.match(name):
            # replace only the matched prefix (case-insensitive) with short
            return pattern.sub(short, name, count=1)
    return name

_IPV4 = r"([0-9]{1,3}(?:\.[0-9]{1,3}){3})"
_IPV6 = r"([0-9A-Fa-f:]+)"

def _find_mgmt_ip(block: str):
    """
    คืน IPv4 ก่อน ถ้าไม่พบค่อยลอง IPv6
    รองรับทั้ง:
      - 'Management Address: 10.30.6.100'
      - 'Management Addresses:\n    IP: 10.30.6.100'
      - บางอุปกรณ์อาจเขียน 'IPv6:' แทน 'IP:'
    """
    # แบบบรรทัดเดียว
    m = re.search(rf"Management Address(?:es)?:\s*(?:IP:\s*)?{_IPV4}", block, re.I)
    if m:
        return m.group(1)

    # แบบหลายบรรทัด: 'Management Addresses:' แล้วค่อยมี 'IP: x.x.x.x'
    m = re.search(rf"Management Addresses:\s*(?:\r?\n)+\s*IP:\s*{_IPV4}", block, re.I)
    if m:
        return m.group(1)

    # fallback IPv6 (ถ้าจำเป็น)
    m = re.search(rf"Management Address(?:es)?:\s*(?:IPv6:\s*)?{_IPV6}", block, re.I)
    if m:
        return m.group(1)

    m = re.search(rf"Management Addresses:\s*(?:\r?\n)+\s*IPv6:\s*{_IPV6}", block, re.I)
    if m:
        return m.group(1)

    return None

def parse_lldp_cisco(text: str):
    """
    Return list of dict entries with keys:
      local_if, remote_sysname, remote_portdesc, remote_mgmt_ip, device_type
    """
    blocks = re.split(r"-{5,}", text)
    out = []
    for b in blocks:
        if not b or not b.strip():
            continue
        local_if = re.search(r"Local Intf:\s*([\w\/\.]+)", b)
        sysname  = re.search(r"System Name:\s*([^\r\n]+)", b)
        portdesc = re.search(r"Port Description:\s*([^\r\n]+)", b)
        sys_caps = re.search(r"System Capabilities:\s*([^\r\n]+)", b)
        en_caps  = re.search(r"Enabled Capabilities:\s*([^\r\n]+)", b)
        if local_if and sysname:
            mgmt_ip = _find_mgmt_ip(b)
            dev_type = classify_from_lldp_caps(sys_caps.group(1) if sys_caps else '', en_caps.group(1) if en_caps else '')
            if dev_type == 'unknown':
                print(f"[LLDP] device_type unknown for neighbor '{sysname.group(1).strip()}' caps=({sys_caps.group(1).strip() if sys_caps else ''}) enabled=({en_caps.group(1).strip() if en_caps else ''})")
            out.append({
                "local_if": normalize_if_name(local_if.group(1).strip()),
                "remote_sysname": sysname.group(1).strip(),
                "remote_port": normalize_if_name(portdesc.group(1).strip()) if portdesc else "",
                "remote_mgmt_ip": mgmt_ip,
                "device_type": dev_type
            })
    return out

def parse_cdp_cisco(text: str):
    """
    Return list of dict entries with keys:
      local_if, remote_sysname, remote_port, remote_mgmt_ip, device_type
    """
    out = []
    blocks = re.split(r"Device ID:\s*", text)
    for b in blocks[1:]:
        lines = [ln for ln in b.splitlines() if ln.strip()]
        if not lines:
            continue
        sysname = lines[0].strip()
        local_if = re.search(r"Interface:\s*([\w\/\.]+),", b)
        portid   = re.search(r"Port ID \(outgoing port\):\s*([^\r\n]+)", b)
        mgmt_ip  = re.search(rf"IP address:\s*{_IPV4}", b, re.I)
        caps     = re.search(r"Capabilities:\s*([^\r\n]+)", b, re.I)
        if local_if and sysname:
            dev_type = classify_from_cdp_caps(caps.group(1) if caps else '')
            if dev_type == 'unknown':
                print(f"[CDP] device_type unknown for neighbor '{sysname}' caps=({caps.group(1).strip() if caps else ''})")
            out.append({
                "local_if": normalize_if_name(local_if.group(1).strip()),
                "remote_sysname": sysname,
                "remote_port": normalize_if_name(portid.group(1).strip()) if portid else "",
                "remote_mgmt_ip": (mgmt_ip.group(1).strip() if mgmt_ip else None),
                "device_type": dev_type
            })
    return out
