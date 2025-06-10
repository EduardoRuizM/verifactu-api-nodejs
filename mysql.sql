CREATE TABLE IF NOT EXISTS `companies` (
  `id` int(11) unsigned NOT NULL auto_increment PRIMARY KEY,
  `code` varchar(25) NOT NULL UNIQUE,
  `name` varchar(50) NOT NULL,
  `vat_id` varchar(25) NOT NULL UNIQUE,
  `address` varchar(75) NOT NULL,
  `postal_code` varchar(10) NOT NULL,
  `city` varchar(25) NOT NULL,
  `state` varchar(25) NOT NULL,
  `country` char(2) NOT NULL DEFAULT 'ES',
  `email` varchar(50),
  `phone` varchar(50),
  `contact` varchar(50),
  `formula` varchar(25) NOT NULL DEFAULT '%n%',
  `formula_r` varchar(25) NOT NULL DEFAULT 'R-%n%',
  `first_num` int(11) unsigned NOT NULL DEFAULT '1',
  `created` date NOT NULL,
  `next_send` datetime,
  `test` tinyint(1) NOT NULL DEFAULT '1'
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS `invoices` (
  `id` int(11) unsigned NOT NULL auto_increment PRIMARY KEY,
  `company_id` int(11) unsigned NOT NULL,
  `dt` datetime NOT NULL default CURRENT_TIMESTAMP,
  `num` int(11) unsigned NOT NULL,
  `name` varchar(50) NOT NULL,
  `vat_id` varchar(25),
  `address` varchar(75),
  `postal_code` varchar(10),
  `city` varchar(25),
  `state` varchar(25),
  `country` char(2),
  `tvat` double NOT NULL DEFAULT '0',
  `bi` double NOT NULL DEFAULT '0',
  `total` double NOT NULL DEFAULT '0',
  `email` varchar(50),
  `ref` varchar(25),
  `comments` text,
  `fingerprint` varchar(64),
  `verifactu_type` char(2),
  `verifactu_stype` char(1),
  `verifactu_dt` timestamp NULL DEFAULT NULL,
  `verifactu_csv` text,
  `verifactu_err` int(11) unsigned,
  `invoice_ref_id` int(11) unsigned,
  `voided` tinyint(1) NOT NULL DEFAULT '0',
  KEY `dt` (`dt`),
  KEY `num` (`num`),
  KEY `fingerprint` (`fingerprint`),
  KEY `verifactu_type` (`verifactu_type`),
  KEY `verifactu_dt` (`verifactu_dt`),
  FOREIGN KEY (`company_id`) REFERENCES `companies` (`id`) ON DELETE RESTRICT,
  FOREIGN KEY (`invoice_ref_id`) REFERENCES `invoices` (`id`) ON DELETE RESTRICT
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS `invoice_lines` (
  `invoice_id` int(11) unsigned NOT NULL,
  `num` int(11) unsigned NOT NULL DEFAULT '0',
  `descr` varchar(100),
  `units` int(11),
  `price` double,
  `vat` int(11) unsigned,
  `tvat` double,
  `bi` double,
  `total` double,
  PRIMARY KEY  (`invoice_id`, `num`),
  KEY `num` (`num`),
  FOREIGN KEY (`invoice_id`) REFERENCES `invoices` (`id`) ON DELETE CASCADE
) ENGINE=InnoDB;

-- Datos de prueba
INSERT IGNORE INTO `companies` SET code='demo', name='Empresa de prueba', vat_id='B53333', country='ES', formula='%y%/%n.8%', created=CURRENT_DATE;
