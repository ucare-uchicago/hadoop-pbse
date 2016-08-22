import table
MAPS = table.MAPS

FROM = MAPS
WHERE = lambda map: not map['attempt'].endswith('_0') and map['datanode'] != []
SELECT = lambda map: (map['attempt'], map['mapnode'], map['datanode'][-1])
