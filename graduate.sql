# hadoop table
create table if not exists hadoop (
    id int NOT NULL AUTO_INCREMENT,
    pcaResult text NOT NULL,
    PRIMARY KEY (id)
) default charset=utf8;
