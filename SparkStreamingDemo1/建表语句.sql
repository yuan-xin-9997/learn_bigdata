CREATE TABLE `wordcount` (
  `word` varchar(100) NOT NULL,
  `count` bigint(20) DEFAULT NULL,
  PRIMARY KEY (`word`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8

CREATE TABLE `offsets` (
  `groupID` varchar(100) NOT NULL,
  `topic` varchar(100) NOT NULL,
  `partitionId` int(11) NOT NULL,
  `offset` bigint(20) DEFAULT NULL,
  PRIMARY KEY (`groupID`,`topic`,`partitionId`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8