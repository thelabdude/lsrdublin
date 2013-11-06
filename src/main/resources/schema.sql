/*
DROP TABLE IF EXISTS page;
CREATE TABLE page (
  id VARCHAR(10) NOT NULL PRIMARY KEY,
  url TEXT NOT NULL,
  host VARCHAR(255) NOT NULL,
  created_on TIMESTAMP DEFAULT NOW()
) ENGINE=innodb CHARACTER SET=utf8 COLLATE utf8_general_ci;
*/

DROP TABLE IF EXISTS page_count;
CREATE TABLE page_count (
  page_id VARCHAR(10) NOT NULL,
  window_timestamp TIMESTAMP NOT NULL,
  window_size_secs INT NOT NULL,
  freq INT NOT NULL,
  PRIMARY KEY(page_id,window_timestamp)
) ENGINE=innodb CHARACTER SET=utf8 COLLATE utf8_general_ci;