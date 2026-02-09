# src/netops/ipam/subnetting.py

from __future__ import annotations
import ipaddress
from dataclasses import dataclass
from typing import Iterable, Optional

@dataclass(frozen=True)
class SubnetInfo:
    network: ipaddress.IPv4Network
    broadcast: ipaddress.IPv4Address
    gateway: ipaddress.IPv4Address
    first_usable: ipaddress.IPv4Address
    last_usable: ipaddress.IPv4Address

    @property
    def cidr(self) -> str:
        return f"{self.network.network_address}/{self.network.prefixlen}"

    @property
    def usable_range(self) -> str:
        return f"{self.first_usable}-{self.last_usable}"

def parse_network(s: str, prefix: int) -> ipaddress.IPv4Network:
    # Accept "192.168.96.0" or "192.168.96.0/28"
    if "/" in s:
        n = ipaddress.ip_network(s, strict=False)
        if n.prefixlen != prefix:
            n = ipaddress.ip_network(f"{n.network_address}/{prefix}", strict=True)
        return n
    return ipaddress.ip_network(f"{s}/{prefix}", strict=True)

def nth_subnet(base: ipaddress.IPv4Network, n: int, prefix: int) -> ipaddress.IPv4Network:
    step = 1 << (32 - prefix)
    start_int = int(base.network_address) + (n * step)
    return ipaddress.ip_network((ipaddress.IPv4Address(start_int), prefix), strict=True)

def describe_subnet(
    net: ipaddress.IPv4Network,
    *,
    gateway_offset: int = 1,
) -> SubnetInfo:
    bcast = net.broadcast_address
    gw = ipaddress.IPv4Address(int(net.network_address) + gateway_offset)

    # hosts() yields usable hosts, safe for typical subnets
    hosts = list(net.hosts())
    if not hosts:
        raise ValueError(f"No usable hosts in {net}")
    first_usable = hosts[0]
    last_usable = hosts[-1]

    return SubnetInfo(
        network=net,
        broadcast=bcast,
        gateway=gw,
        first_usable=first_usable,
        last_usable=last_usable,
    )

def iter_subnets(
    base_net: str,
    *,
    prefix: int,
    count: int = 1,
    start_index: int = 0,
    gateway_offset: int = 1,
) -> Iterable[SubnetInfo]:
    base = parse_network(base_net, prefix)
    for i in range(start_index, start_index + count):
        net = nth_subnet(base, i, prefix)
        yield describe_subnet(net, gateway_offset=gateway_offset)

def vlan_to_index(vlan: int, *, base_vlan: int = 1001) -> int:
    """
    VLAN 1001 -> 0, VLAN 1002 -> 1, etc.
    """
    return vlan - base_vlan

def subnet_for_vlan(*, vlan: int, base_vlan: int, base_net: str, prefix: int) -> SubnetInfo:
    idx = vlan - base_vlan
    if idx < 0:
        raise ValueError(f"VLAN {vlan} is below base VLAN {base_vlan}")
    base = parse_network(base_net, prefix)
    net = nth_subnet(base, idx, prefix)
    return describe_subnet(net)
