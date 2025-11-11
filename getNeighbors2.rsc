:global matches [/ip neighbor print as-value];
:foreach match in=$matches do=[:put (($match->"identity") . "," . ($match->"interface") . "," . ($match->"address") . "," . ($match->"mac-address") . ";");]
