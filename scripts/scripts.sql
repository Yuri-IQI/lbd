CREATE DATABASE space_comex_principal;

\c space_comex_principal;

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
	descricao varchar NOT NULL,
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
	"data" date NOT NULL,
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

-- Inserindo categorias de produtos
INSERT INTO public.categoria_produtos (descricao) VALUES
('Commodities Agrícolas'),
('Energia'),
('Veículos e Máquinas'),
('Papel e Celulose'),
('Metalurgia');

-- Inserindo moedas
INSERT INTO public.moedas (descricao, pais) VALUES
('Dólar Americano', 'USA'),
('Real Brasileiro', 'BRA'),
('Euro', 'EUR'),
('Yuan Chinês', 'CHN');

-- Inserindo meios de transporte
INSERT INTO public.transportes (descricao) VALUES
('Marítimo'),
('Rodoviário'),
('Aéreo'),
('Ferroviário');

-- Inserindo países
INSERT INTO public.paises (nome, codigo_iso, bloco_id) VALUES
('Brasil', 'BRA', 1),
('Estados Unidos', 'USA', 3),
('China', 'CHN', 4),
('Alemanha', 'DEU', 2),
('França', 'FRA', 2);

-- Inserindo produtos
INSERT INTO public.produtos (descricao, categoria_id, codigo_ncm) VALUES
('Soja', 1, '12019000'),
('Petróleo Bruto', 2, '27090010'),
('Automóveis', 3, '87032390'),
('Celulose', 4, '47032100'),
('Aço Laminado', 5, '72083910');

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

CREATE DATABASE space_comex_data_mart;

\c space_comex_data_mart;

CREATE TABLE DM_Produtos (
    "id_produto" bigint NOT NULL,
    "descricao" varchar,
    "codigo_ncm" varchar,
    "ds_categoria" varchar,
    PRIMARY KEY ("id_produto")
);



CREATE TABLE DM_Tempo (
    "id_tempo" bigint NOT NULL,
    "data_completa" date NOT NULL,
    "ano" integer,
    "mes" integer,
    "dia" integer,
    "trimestre" varchar,
    "semestre" varchar,
    PRIMARY KEY ("id_tempo")
);



CREATE TABLE DM_Pais (
    "id_pais" bigint NOT NULL,
    "pais" varchar,
    "codigo_iso" varchar,
    "nm_bloco" bigint,
    PRIMARY KEY ("id_pais")
);



CREATE TABLE DM_Cambios (
    "id_cambio" bigint NOT NULL,
    "data" date NOT NULL,
    "ds_moeda_origem" varchar NOT NULL,
    "pais_moeda_origem" varchar NOT NULL,
    "ds_moeda_destino" varchar NOT NULL,
    "pais_moeda_destino" varchar NOT NULL,
    "taxa_cambio" real NOT NULL,
    PRIMARY KEY ("id_cambio")
);



CREATE TABLE DM_Transporte (
    "id_transporte" bigint NOT NULL,
    "ds_transporte" varchar NOT NULL,
    "tp_transporte" varchar NOT NULL,
    PRIMARY KEY ("id_transporte")
);

CREATE TABLE FT_Transacoes (
    "id_transporte" bigint NOT NULL REFERENCES "DM_Transporte" ("id_transporte"),
    "id_pais_origem" bigint NOT NULL REFERENCES "DM_Pais" ("id_pais"),
    "id_pais_destino" bigint NOT NULL REFERENCES "DM_Pais" ("id_pais"),
    "id_produto" bigint NOT NULL REFERENCES "DM_Produtos" ("id_produto"),
    "id_tempo" bigint NOT NULL REFERENCES "DM_Tempo" ("id_tempo"),
    "id_cambios" bigint NOT NULL REFERENCES "DM_Cambios" ("id_cambio"),
    "valor_monetario" double precision NOT NULL,
    "quantidade" bigint NOT NULL,
    PRIMARY KEY ("id_transporte", "id_pais_origem", "id_pais_destino", "id_produto", "id_tempo", "id_cambios")
);