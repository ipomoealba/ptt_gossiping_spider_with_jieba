CREATE DATABASE `ptt`
 DEFAULT CHARACTER SET utf8
   DEFAULT COLLATE utf8_general_ci;

SET CHARSET 'utf8';
SET NAMES 'utf8';
USE `ptt`;

CREATE TABLE `Gossiping`
(
   pid                varchar(30) NOT NULL,
   title              varchar(100) DEFAULT '',
   ptime              datetime,
   arthor             varchar(30) DEFAULT '',
   ip                 varchar(16) DEFAULT '',
   content_keywords   mediumtext,
   comment_keywords   mediumtext,
   push               int(5) DEFAULT 0,
   sheee              int(5) DEFAULT 0,
   arrow              int(5) DEFAULT 0,
   first_page         tinyint(1) DEFAULT 0,
   relink             int(4) DEFAULT 0,
   PRIMARY KEY(pid)
);

CREATE TABLE `Content`
(
   pid       varchar(30) NOT NULL,
   content   text,
   PRIMARY KEY(pid)
);

CREATE TABLE `Keywords`
(
   pid                varchar(30) NOT NULL,
   content_keywords   text,
   comment_keywords   text,
   PRIMARY KEY(pid)
);

CREATE TABLE `Reply`
(
   pid     varchar(30) NOT NULL,
   reply   text,
   PRIMARY KEY(pid)
);