-- 1. Verificar os catálogos disponíveis
SHOW CATALOGS;

-- 2. Verificar os schemas disponíveis no catálogo hive
SHOW SCHEMAS FROM hive;

-- 3. Listar tabelas do schema default no Hive
SHOW TABLES FROM hive.default;

-- 4. Selecionar dados da tabela vendas
SELECT * FROM hive.default.vendas LIMIT 5;
