/interface vlan 
add interface={identity}_{vlan} name={identity} vlan-id={vlan}
/ip pool
add name=Pool-{identity} ranges={subnet}.2-{subnet}.254
/ip dhcp-server
add address-pool=Pool-{identity} interface={identity} lease-time=23h59m59s name=DHCP-{identity}
/interface wifi security multi-passphrase
add disabled=no isolation=yes group=all_Pad vlan-id={vlan} comment={identity} passphrase={password}
/ip address
add address={subnet}.1/24 interface={identity} network={subnet}.0
/ip dhcp-server network
add address={subnet}.0/24 comment={identity} dns-server=8.8.8.8 gateway={subnet}.1
/queue simple
add limit-at={speed}M/{speed}M max-limit={speed}M/{speed}M name={identity} target={subnet}.0/24