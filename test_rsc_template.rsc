/interface vlan 
add interface=Bridge_Trunk name={identity} vlan-id={vlan}
/ip pool
add name=Pool-{identity} ranges={pool}
/ip dhcp-server
add address-pool=Pool-{identity} interface={identity} lease-time=23h59m59s name=DHCP-{identity}
/interface wifi security multi-passphrase
add disabled=no isolation=yes group=all_Pad vlan-id={vlan} comment={identity} passphrase="DefaultPassphrase"
/ip address
add address={gateway}/{prefix} interface={identity} network={network}
/ip dhcp-server network
add address={network}/{prefix} comment={identity} dns-server=8.8.8.8 gateway={gateway}
/queue simple
add limit-at={speed}M/{speed}M max-limit={speed}M/{speed}M name={identity} target={network}/{prefix}