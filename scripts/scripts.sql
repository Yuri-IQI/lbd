CREATE DATABASE star_comex_principal;

\c star_comex_principal;

-- public.blocos_economicos definição

-- Drop table

-- DROP TABLE public.blocos_economicos;

CREATE TABLE public.blocos_economicos (
	id serial4 NOT NULL,
	nome varchar(100) NOT NULL,
	CONSTRAINT blocos_economicos_pkey PRIMARY KEY (id)
);


-- public.categoria_produtos definição

-- Drop table

-- DROP TABLE public.categoria_produtos;

CREATE TABLE public.categoria_produtos (
	id serial4 NOT NULL,
	descricao varchar(255) NOT NULL,
	CONSTRAINT categoria_produtos_pkey PRIMARY KEY (id)
);


-- public.moedas definição

-- Drop table

-- DROP TABLE public.moedas;

CREATE TABLE public.moedas (
	id serial4 NOT NULL,
	descricao varchar(255) NOT NULL,
	pais varchar(10) NOT NULL,
	CONSTRAINT moedas_pkey PRIMARY KEY (id)
);


-- public.tipos_transacoes definição

-- Drop table

-- DROP TABLE public.tipos_transacoes;

CREATE TABLE public.tipos_transacoes (
	id serial4 NOT NULL,
	descricao varchar(10) NOT NULL,
	CONSTRAINT tipos_transacoes_pkey PRIMARY KEY (id)
);


-- public.transportes definição

-- Drop table

-- DROP TABLE public.transportes;

CREATE TABLE public.transportes (
	id serial4 NOT NULL,
	descricao varchar(50) NOT NULL,
	CONSTRAINT transportes_pkey PRIMARY KEY (id)
);


-- public.cambios definição

-- Drop table

-- DROP TABLE public.cambios;

CREATE TABLE public.cambios (
	id serial4 NOT NULL,
	data date NOT NULL,
	moeda_origem int4 NOT NULL,
	moeda_destino int4 NOT NULL,
	taxa_cambio numeric(10, 4) NOT NULL,
	CONSTRAINT cambios_data_moeda_origem_moeda_destino_key UNIQUE (data, moeda_origem, moeda_destino),
	CONSTRAINT cambios_pkey PRIMARY KEY (id),
	CONSTRAINT fk_moeda_destino FOREIGN KEY (moeda_destino) REFERENCES public.moedas(id) ON DELETE CASCADE,
	CONSTRAINT fk_moeda_origem FOREIGN KEY (moeda_origem) REFERENCES public.moedas(id) ON DELETE CASCADE
);


-- public.paises definição

-- Drop table

-- DROP TABLE public.paises;

CREATE TABLE public.paises (
	id serial4 NOT NULL,
	nome varchar(100) NOT NULL,
	codigo_iso bpchar(3) NOT NULL,
	bloco_id int4 NOT NULL,
	CONSTRAINT paises_codigo_iso_key UNIQUE (codigo_iso),
	CONSTRAINT paises_pkey PRIMARY KEY (id),
	CONSTRAINT fk_blocos_economicos FOREIGN KEY (bloco_id) REFERENCES public.blocos_economicos(id) ON DELETE CASCADE
);


-- public.produtos definição

-- Drop table

-- DROP TABLE public.produtos;

CREATE TABLE public.produtos (
	id serial4 NOT NULL,
	descricao varchar(255) NOT NULL,
	categoria_id int4 NOT NULL,
	codigo_ncm varchar(10) NOT NULL,
	CONSTRAINT produtos_codigo_ncm_key UNIQUE (codigo_ncm),
	CONSTRAINT produtos_pkey PRIMARY KEY (id),
	CONSTRAINT produtos_categoria_produtos_fk FOREIGN KEY (categoria_id) REFERENCES public.categoria_produtos(id)
);


-- public.transacoes definição

-- Drop table

-- DROP TABLE public.transacoes;

CREATE TABLE public.transacoes (
	id serial4 NOT NULL,
	tipo_id int4 NOT NULL,
	pais_origem int4 NOT NULL,
	pais_destino int4 NOT NULL,
	produto_id int4 NOT NULL,
	valor_monetario numeric(15, 2) NOT NULL,
	quantidade int4 NOT NULL,
	transporte_id int4 NOT NULL,
	cambio_id int4 NOT NULL,
	CONSTRAINT transacoes_pkey PRIMARY KEY (id),
	CONSTRAINT fk_cambio FOREIGN KEY (cambio_id) REFERENCES public.cambios(id) ON DELETE CASCADE,
	CONSTRAINT fk_pais_destino FOREIGN KEY (pais_destino) REFERENCES public.paises(id) ON DELETE CASCADE,
	CONSTRAINT fk_pais_origem FOREIGN KEY (pais_origem) REFERENCES public.paises(id) ON DELETE CASCADE,
	CONSTRAINT fk_produto FOREIGN KEY (produto_id) REFERENCES public.produtos(id) ON DELETE CASCADE,
	CONSTRAINT fk_tipos_transacoes FOREIGN KEY (tipo_id) REFERENCES public.tipos_transacoes(id) ON DELETE CASCADE,
	CONSTRAINT fk_transportes FOREIGN KEY (transporte_id) REFERENCES public.transportes(id) ON DELETE CASCADE
);

-- Inserindo blocos econômicos
INSERT INTO public.blocos_economicos (nome) VALUES
('Mercosul'),
('União Europeia'),
('NAFTA'),
('BRICS');

INSERT INTO public.blocos_economicos (nome) VALUES
('ASEAN'),
('União Africana');

-- Inserindo categorias de produtos
INSERT INTO public.categoria_produtos (descricao) VALUES
('Commodities Agrícolas'),
('Energia'),
('Veículos e Máquinas'),
('Papel e Celulose'),
('Metalurgia');

INSERT INTO public.categoria_produtos (descricao) VALUES
('Tecnologia'),
('Produtos Químicos');

-- Inserindo moedas
INSERT INTO public.moedas (descricao, pais) VALUES
('Dólar Americano', 'USA'),
('Real Brasileiro', 'BRA'),
('Euro', 'EUR'),
('Yuan Chinês', 'CHN');

INSERT INTO public.moedas (descricao, pais) VALUES
('Iene Japonês', 'JPN'),
('Libra Esterlina', 'GBR');

-- Inserindo meios de transporte
INSERT INTO public.transportes (descricao) VALUES
('Marítimo'),
('Rodoviário'),
('Aéreo'),
('Ferroviário');

INSERT INTO public.transportes (descricao) VALUES
('Dutoviário'),
('Fluvial');

-- Inserindo países
INSERT INTO public.paises (nome, codigo_iso, bloco_id) VALUES
('Brasil', 'BRA', 1),
('Estados Unidos', 'USA', 3),
('China', 'CHN', 4),
('Alemanha', 'DEU', 2),
('França', 'FRA', 2);

INSERT INTO public.paises (nome, codigo_iso, bloco_id) VALUES
('Japão', 'JPN', 1),
('Reino Unido', 'GBR', 2),
('Nigéria', 'NGA', 6),
('Índia', 'IND', 4),
('Indonésia', 'IDN', 5);

-- Inserindo produtos
INSERT INTO public.produtos (descricao, categoria_id, codigo_ncm) VALUES
('Soja', 1, '12019000'),
('Petróleo Bruto', 2, '27090010'),
('Automóveis', 3, '87032390'),
('Celulose', 4, '47032100'),
('Aço Laminado', 5, '72083910');

INSERT INTO public.produtos (descricao, categoria_id, codigo_ncm) VALUES
('Smartphones', 6, '85171231'),
('Fertilizantes Nitrogenados', 7, '31021010');

-- Inserindo tipos_transacoes
INSERT INTO public.tipos_transacoes (descricao) VALUES
('IMPORT'),
('EXPORT');

-- Inserindo taxas de câmbio de 2020 a 2024
INSERT INTO public.cambios (data, moeda_origem, moeda_destino, taxa_cambio) VALUES
-- Ano 2020
('2020-03-15', 1, 2, 4.50),  -- USD -> BRL
('2020-07-20', 3, 2, 5.20),  -- EUR -> BRL
('2020-10-05', 4, 1, 0.14),  -- CNY -> USD
('2021-02-10', 1, 2, 5.30),  -- USD -> BRL
('2021-06-18', 3, 2, 6.10),  -- EUR -> BRL
('2021-09-30', 4, 3, 0.13),  -- CNY -> EUR
('2022-01-25', 1, 2, 5.00),  -- USD -> BRL
('2022-08-14', 3, 1, 1.08),  -- EUR -> USD
('2023-04-12', 1, 3, 0.92),  -- USD -> EUR
('2023-11-22', 2, 1, 0.19);  -- BRL -> USD

-- Câmbios adicionais
INSERT INTO public.cambios (data, moeda_origem, moeda_destino, taxa_cambio) VALUES
-- 2020
('2020-11-03', 2, 3, 0.18),   -- BRL -> EUR

-- 2021
('2021-08-22', 1, 4, 6.45),   -- USD -> CNY

-- 2022
('2022-05-10', 5, 2, 0.042),  -- GBP -> BRL

-- 2023
('2023-09-28', 6, 5, 155.60), -- GBP -> JPY

-- 2024
('2024-11-12', 4, 6, 0.12),   -- CNY -> GBP

-- 2025
('2025-01-14', 3, 5, 0.87);   -- EUR -> GBP


INSERT INTO public.cambios (data, moeda_origem, moeda_destino, taxa_cambio) VALUES
-- Ano 2024
('2024-01-10', 1, 6, 110.45), -- USD -> JPY
('2024-03-05', 3, 6, 0.85),   -- EUR -> GBP
('2024-05-22', 6, 1, 0.0091), -- JPY -> USD
('2024-07-15', 6, 3, 1.17),   -- GBP -> EUR
('2024-10-30', 2, 6, 22.15);  -- BRL -> JPY

INSERT INTO public.cambios (data, moeda_origem, moeda_destino, taxa_cambio) VALUES
-- 2018
('2018-02-15', 1, 2, 3.25),  -- USD -> BRL
('2018-06-30', 3, 1, 1.18),  -- EUR -> USD

-- 2019
('2019-03-10', 1, 6, 111.20),  -- USD -> JPY
('2019-08-20', 6, 3, 0.0078),  -- JPY -> EUR

-- 2020
('2020-12-01', 6, 1, 1.33),  -- GBP -> USD

-- 2021
('2021-05-10', 2, 6, 0.15),  -- BRL -> GBP

-- 2022
('2022-03-18', 3, 6, 129.00),  -- EUR -> JPY
('2022-09-07', 6, 2, 0.048),  -- JPY -> BRL

-- 2023
('2023-01-23', 4, 6, 0.11),   -- CNY -> GBP
('2023-06-17', 6, 4, 8.90),   -- GBP -> CNY

-- 2024
('2024-02-12', 1, 3, 0.94),   -- USD -> EUR
('2024-09-09', 6, 2, 6.22),   -- GBP -> BRL

-- 2025
('2025-03-05', 2, 6, 20.75),  -- BRL -> JPY
('2025-04-07', 5, 6, 0.0061); -- JPY -> GBP

INSERT INTO public.cambios (data, moeda_origem, moeda_destino, taxa_cambio) VALUES
('2020-04-09', 5, 3, 1.17),    -- GBP -> EUR
('2020-08-16', 3, 4, 7.85),    -- EUR -> CNY
('2020-12-19', 2, 4, 1.50),    -- BRL -> CNY
('2021-03-11', 6, 2, 0.051),   -- JPY -> BRL
('2021-07-25', 4, 5, 0.11),    -- CNY -> GBP
('2021-10-28', 3, 2, 6.35),    -- EUR -> BRL
('2022-01-08', 1, 5, 0.76),    -- USD -> GBP
('2022-04-22', 6, 1, 0.0089),  -- JPY -> USD
('2022-07-30', 5, 4, 8.90),    -- GBP -> CNY
('2022-11-18', 2, 1, 0.21),    -- BRL -> USD
('2023-02-05', 4, 2, 0.71),    -- CNY -> BRL
('2023-05-15', 3, 6, 151.00),  -- EUR -> JPY
('2023-08-25', 5, 3, 1.11),    -- GBP -> EUR
('2023-10-13', 1, 4, 6.90),    -- USD -> CNY
('2024-01-21', 2, 5, 0.16),    -- BRL -> GBP
('2024-04-11', 6, 3, 0.0068),  -- JPY -> EUR
('2024-06-18', 3, 1, 1.07),    -- EUR -> USD
('2024-08-29', 5, 6, 160.30),  -- GBP -> JPY
('2025-02-19', 4, 1, 0.15),    -- CNY -> USD
('2025-03-28', 6, 2, 0.045);   -- JPY -> BRL

-- Inserindo transações comerciais para cada taxa de câmbio
INSERT INTO public.transacoes (tipo_id, pais_origem, pais_destino, produto_id, valor_monetario, quantidade, transporte_id, cambio_id) VALUES
(2, 1, 2, 1, 4500000.00, 12000, 1, 1),
(1, 2, 1, 3, 7800000.00, 6000, 2, 1),
(2, 3, 4, 5, 9000000.00, 3000, 3, 1),
(1, 4, 5, 2, 5600000.00, 4000, 1, 1),
(2, 5, 3, 4, 7300000.00, 5000, 2, 1),
(2, 1, 3, 2, 8000000.00, 4000, 1, 4),
(1, 2, 4, 5, 9200000.00, 5000, 3, 4),
(2, 3, 5, 3, 11000000.00, 7000, 2, 4),
(1, 4, 1, 1, 5200000.00, 3000,  4, 4),
(2, 5, 2, 4, 7600000.00, 4000, 1, 4),
(2, 1, 5, 3, 10000000.00, 6000,  2, 7),
(1, 2, 3, 4, 8500000.00, 5000,  3, 7),
(2, 3, 2, 1, 4200000.00, 2000,  4, 7),
(1, 4, 1, 5, 9600000.00, 4000,  1, 7),
(2, 5, 4, 2, 8800000.00, 4500, 3, 7),
(2, 1, 4, 1, 6200000.00, 3100, 2, 9),
(1, 2, 5, 3, 7800000.00, 4500, 3, 9),
(2, 3, 1, 2, 8900000.00, 5000, 4, 9),
(1, 4, 2, 4, 7100000.00, 3800, 1, 9),
(2, 5, 3, 5, 9700000.00, 5500, 3, 9);

INSERT INTO public.transacoes (tipo_id, pais_origem, pais_destino, produto_id, valor_monetario, quantidade, transporte_id, cambio_id) VALUES
(2, 6, 1, 6, 15000000.00, 9000, 1, 11), -- Japão exporta Smartphones para Brasil
(1, 1, 6, 7, 6200000.00, 7000, 5, 11),  -- Brasil importa Fertilizantes do Japão

(2, 4, 7, 6, 8700000.00, 4000, 6, 12), -- Alemanha exporta Smartphones para Reino Unido
(1, 7, 3, 7, 4300000.00, 3500, 2, 12), -- Reino Unido exporta Fertilizantes para China

(1, 6, 2, 6, 9800000.00, 5000, 1, 13), -- Japão -> EUA (Smartphones)
(2, 2, 6, 7, 4700000.00, 3000, 4, 13), -- EUA -> Japão (Fertilizantes)

(2, 7, 3, 6, 9100000.00, 4000, 3, 14), -- Reino Unido -> China
(1, 3, 7, 7, 6200000.00, 2000, 1, 14), -- China -> Reino Unido

(2, 1, 6, 7, 3000000.00, 1000, 6, 15), -- Brasil -> Japão (Fertilizantes)
(1, 6, 1, 6, 8900000.00, 4200, 2, 15); -- Japão -> Brasil (Smartphones)

-- Transações para as novas taxas de câmbio
INSERT INTO public.transacoes (tipo_id, pais_origem, pais_destino, produto_id, valor_monetario, quantidade, transporte_id, cambio_id) VALUES
-- 2020
(1, 2, 3, 2, 3500000.00, 2400, 2, 30),  -- Brasil -> Alemanha

-- 2021
(2, 1, 4, 4, 4800000.00, 5000, 1, 31),  -- EUA -> China

-- 2022
(1, 5, 2, 3, 3200000.00, 1500, 3, 32),  -- Reino Unido -> Brasil

-- 2023
(2, 6, 5, 1, 5600000.00, 2600, 4, 33),  -- Japão -> Reino Unido

-- 2024
(1, 4, 6, 5, 6100000.00, 3100, 2, 34),  -- China -> Japão

-- 2025
(2, 3, 5, 6, 4400000.00, 2700, 1, 35);  -- Alemanha -> Reino Unido

INSERT INTO public.transacoes (tipo_id, pais_origem, pais_destino, produto_id, valor_monetario, quantidade, transporte_id, cambio_id) VALUES
-- Relativas ao ano de 2018
(1, 2, 1, 3, 5000000.00, 3000, 2, 16),
(2, 4, 2, 5, 7200000.00, 3500, 1, 17),

-- 2019
(2, 6, 1, 6, 8300000.00, 4000, 5, 18),
(1, 1, 6, 7, 2900000.00, 1500, 4, 19),

-- 2020
(2, 7, 2, 4, 7500000.00, 2800, 1, 20),

-- 2021
(1, 2, 7, 3, 6400000.00, 2700, 6, 21),

-- 2022
(2, 3, 6, 1, 9100000.00, 3100, 3, 22),
(1, 6, 2, 5, 3000000.00, 1800, 1, 23),

-- 2023
(2, 4, 7, 2, 8800000.00, 3300, 4, 24),
(1, 7, 3, 6, 9900000.00, 5000, 2, 25),

-- 2024
(1, 1, 3, 1, 8200000.00, 3700, 3, 26),
(2, 7, 2, 7, 6100000.00, 4200, 5, 27),

-- 2025
(1, 2, 6, 3, 5700000.00, 3100, 6, 28),
(2, 6, 7, 4, 4300000.00, 2300, 1, 29);

INSERT INTO public.transacoes (tipo_id, pais_origem, pais_destino, produto_id, valor_monetario, quantidade, transporte_id, cambio_id) VALUES
(1, 5, 3, 2, 5300000.00, 2700, 3, 36),   -- Reino Unido -> Alemanha
(2, 3, 4, 3, 7700000.00, 3400, 2, 37),   -- Alemanha -> China
(1, 2, 4, 4, 6200000.00, 3900, 4, 38),   -- Brasil -> China
(2, 6, 2, 1, 2500000.00, 1200, 1, 39),   -- Japão -> Brasil
(1, 4, 5, 5, 4800000.00, 2000, 6, 40),   -- China -> Reino Unido
(2, 3, 2, 2, 9100000.00, 5200, 3, 41),   -- Alemanha -> Brasil
(1, 1, 5, 4, 5700000.00, 3100, 2, 42),   -- EUA -> Reino Unido
(2, 6, 1, 6, 8800000.00, 4700, 5, 43),   -- Japão -> EUA
(1, 5, 4, 3, 3600000.00, 1800, 3, 44),   -- Reino Unido -> China
(2, 2, 1, 1, 6800000.00, 2600, 1, 45),   -- Brasil -> EUA
(1, 4, 2, 5, 3400000.00, 2100, 6, 46),   -- China -> Brasil
(2, 3, 6, 6, 9300000.00, 6000, 2, 47),   -- Alemanha -> Japão
(1, 5, 3, 1, 6100000.00, 3300, 3, 48),   -- Reino Unido -> Alemanha
(2, 1, 4, 2, 7500000.00, 4100, 4, 49),   -- EUA -> China
(1, 2, 5, 3, 4200000.00, 2800, 5, 50),   -- Brasil -> Reino Unido
(2, 6, 3, 4, 8300000.00, 4400, 1, 51),   -- Japão -> Alemanha
(1, 3, 1, 5, 9900000.00, 5200, 2, 52),   -- Alemanha -> EUA
(2, 5, 6, 6, 5800000.00, 3000, 6, 53),   -- Reino Unido -> Japão
(1, 4, 1, 2, 6400000.00, 3500, 3, 54),   -- China -> EUA
(2, 6, 2, 3, 3700000.00, 2000, 4, 55);   -- Japão -> Brasil


CREATE DATABASE star_comex_data_mart;

\c star_comex_data_mart;

CREATE TABLE DM_Produtos (
	sk_produto int4 NOT NULL,
	id_produto int4 NOT NULL,
	descricao varchar(255),
	codigo_ncm varchar(10),
	ds_categoria varchar(255),
	PRIMARY KEY (sk_produto)
);

CREATE TABLE DM_Tempo (
	sk_tempo int4 NOT NULL,
	data_completa date NOT NULL,
	ano int4,
	mes int4,
	dia int4,
	PRIMARY KEY (sk_tempo)
);

CREATE TABLE DM_Pais (
	sk_pais int4 NOT NULL,
	id_pais int4 NOT NULL,
	pais varchar(100),
	codigo_iso bpchar(3),
	nm_bloco varchar(100),
	PRIMARY KEY (sk_pais)
);

CREATE TABLE DM_Cambios (
	sk_cambio int4 NOT NULL,
	id_cambio int4 NOT NULL,
	data date NOT NULL,
	ds_moeda_origem varchar(255) NOT NULL,
	cd_moeda_origem varchar(10) NOT NULL,
	ds_moeda_destino varchar(255) NOT NULL,
	cd_moeda_destino varchar(10) NOT NULL,
	taxa_cambio numeric(10,4) NOT NULL,
	PRIMARY KEY (sk_cambio)
);

CREATE TABLE DM_Transporte (
	sk_transporte int4 NOT NULL,
	id_transporte int4 NOT NULL,
	ds_transporte varchar(50) NOT NULL,
	PRIMARY KEY (sk_transporte)
);

CREATE TABLE FT_Transacoes (
	sk_transacao int4 NOT NULL,
	id_transacao int4 NOT NULL,
	sk_transporte int4 NOT NULL REFERENCES DM_Transporte (sk_transporte),
	sk_pais_origem int4 NOT NULL REFERENCES DM_Pais (sk_pais),
	sk_pais_destino int4 NOT NULL REFERENCES DM_Pais (sk_pais),
	sk_produto int4 NOT NULL REFERENCES DM_Produtos (sk_produto),
	sk_tempo int4 NOT NULL REFERENCES DM_Tempo (sk_tempo),
	sk_cambio int4 NOT NULL REFERENCES DM_Cambios (sk_cambio),
	valor_monetario numeric(15,2) NOT NULL,
	quantidade int4 NOT NULL,
	tp_transacao varchar(10),
	PRIMARY KEY (sk_transacao)
);