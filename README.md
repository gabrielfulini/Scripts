# Scripts



-- Script tira linha duplicada da tabela carga.espelho_unificado

BEGIN
    FOR x
        IN (  SELECT es.ID_PESSOA,
                     es.FANTASIA,
                     es.PERIODO,
                     es.PERIODO_INICIAL,
                     es.PERIODO_FINAL,
                     es.SERVICO,
                     es.DESC_SERVICO,
                     es.QTDE,
                     es.VALOR_UNITARIO,
                     es.PORCENTAGEM_ADICIONAL,
                     es.ID_CAIXA,
                     es.DT_ENVIO_IMB,
                     es.ID_SOLICITACAO,
                     es.DT_SOLICITACAO,
                     es.DT_CONCLUSAO,
                     es.VALOR_PORCENTAGEM_ADICIONAL,
                     es.SOMA,
                     COUNT (*)     qtde_linhas
                FROM carga.espelho_unificado
            GROUP BY es.ID_PESSOA,
                     es.FANTASIA,
                     es.PERIODO,
                     es.PERIODO_INICIAL,
                     es.PERIODO_FINAL,
                     es.SERVICO,
                     es.DESC_SERVICO,
                     es.QTDE,
                     es.VALOR_UNITARIO,
                     es.PORCENTAGEM_ADICIONAL,
                     es.ID_CAIXA,
                     es.DT_ENVIO_IMB,
                     es.ID_SOLICITACAO,
                     es.DT_SOLICITACAO,
                     es.DT_CONCLUSAO,
                     es.VALOR_PORCENTAGEM_ADICIONAL,
                     es.SOMA)
    LOOP
        IF x.qtde_linhas > 1
        THEN
            DELETE FROM carga.espelho_unificado
                  WHERE     ID_PESSOA = x.ID_PESSOA
                        AND FANTASIA = x.FANTASIA
                        AND PERIODO = x.PERIODO
                        AND PERIODO_INICIAL = x.PERIODO_INICIAL
                        AND PERIODO_FINAL = x.PERIODO_FINAL
                        AND SERVICO = x.SERVICO
                        AND DESC_SERVICO = x.DESC_SERVICO
                        AND QTDE = x.QTDE
                        AND VALOR_UNITARIO = x.VALOR_UNITARIO
                        AND PORCENTAGEM_ADICIONAL = x.PORCENTAGEM_ADICIONAL
                        AND ID_CAIXA = x.ID_CAIXA
                      --  AND DT_ENVIO_IMB = x.DT_ENVIO_IMB
                        AND ID_SOLICITACAO = x.ID_SOLICITACAO
                        AND DT_SOLICITACAO = x.DT_SOLICITACAO
                        AND DT_CONCLUSAO = x.DT_CONCLUSAO
                        AND VALOR_PORCENTAGEM_ADICIONAL = x.VALOR_PORCENTAGEM_ADICIONAL
                        AND SOMA = x.SOMA
                        AND ROWNUM = 1;
        END IF;
    END LOOP;
END;

______________________________
--For de update

declare
    v_id_local number;
begin
for x in (select * from carga.temp_enviados_imb)
loop
    pkg_migracao_MXA.gerar_posicao (p_resultado => v_id_local);

UPDATE
    caixa
        set status_migracao = 2, dt_preparacao = x.recepção, dt_envio = x.recepção, n_pallet_envio = x.n_pallet_envio, id_local_anterior = id_local, id_local = v_id_local, id_user_acao = 12649
            where id_caixa = x.caixa;
            
commit;
end loop;
end;

____________________________________

script relatório serviço

DECLARE
    v_table_espelho   VARCHAR2 (100);
BEGIN
    FOR x IN (SELECT *
                FROM espelho
               WHERE     id_pessoa =  966
                     AND ano = 2022
                     AND mes in (4,5,6,7,8,9,10,11))
    LOOP
        v_table_espelho := 'CARGA.EXP_TEMP_10_' || x.id_espelho;
        pkg_espelho.pc_external_tb_esp_det_itens (
            p_id_espelho      => x.id_espelho,
            p_id_user_acao    => 10,
            p_tipo_operacao   => 2);
        COMMIT;
        --dbms_output.put_line (v_table_espelho);
        EXECUTE IMMEDIATE 'INSERT INTO carga.ESPELHO_UNIFICADO 
              SELECT p.id_pessoa,
                     p.fantasia,
                     es.mes || ''/'' || es.ano
                         periodo,
                     to_char (es.dt_inicial, ''DD/MM/yyyy'')
                         periodo_inicial,
                     to_char (es.dt_final, ''DD/MM/yyyy'')
                         periodo_final,
                     e.servico,
                     decode (
                         e.servico,
                         ''TRANSPORTE'',    e.desc_servico
                                       || ''-''
                                       || e.id_transporte
                                       || ''-''
                                       || e.id_endereco
                                       || ''-''
                                       || e.numero,
                         e.desc_servico)
                         desc_servico,
                        ROUND (sum (e.qtde),
                                       9)
                         qtde,
                     e.valor_unitario,
                     e.porcentagem_adicional,
                     x.id_caixa,
                     to_char (c.dt_envio, ''DD/MM/yyyy'')
                         dt_envio_imb,
                     e.id_solicitacao,
                     to_char (s.dt_solicitacao, ''DD/MM/yyyy'')
                         dt_solicitacao,
                     to_char (s.dt_conclusao, ''DD/MM/yyyy'')
                         dt_conclusao,
                     sum (x.valor_porcentagem_adicional)
                         valor_porcentagem_adicional,
                     sum (x.valor_total)
                         soma
                FROM espelho_detalhe e,
                     espelho        es,' || v_table_espelho || ' x,
                     caixa          c,
                     solicitacao    s,
                     pessoa         p
               WHERE      e.id_espelho = es.id_espelho
                     AND e.id_servico <> 271
                     AND e.valor_unitario <> 0
                     AND e.id_depto = s.id_depto
                     AND x.id_espelho = e.id_espelho
                     AND x.id_servico = e.id_servico 
                     AND x.id_tipo_item = e.id_tipo_item
                     AND x.id_tipo = e.id_tipo
                     AND x.id_solicitacao = e.id_solicitacao
                     AND x.id_servico <> 18
                     AND x.id_caixa = c.id_caixa (+)
                     AND e.id_solicitacao = s.id_solicitacao
                     AND es.id_pessoa = p.id_pessoa
            GROUP BY p.id_pessoa,
                     p.fantasia,
                     es.mes || ''/''|| es.ano,
                     to_char (es.dt_inicial, ''DD/MM/yyyy''),
                     to_char (es.dt_final, ''DD/MM/yyyy''),
                     e.desc_ax_servico,
                     e.servico,
                     e.id_solicitacao,
                     decode (
                         e.servico,
                         ''TRANSPORTE'',    e.desc_servico
                                       || ''-''
                                       || e.id_transporte
                                       || ''-''
                                       || e.id_endereco
                                       || ''-''
                                       || e.numero,
                         e.desc_servico),
                     e.valor_unitario,
                     e.porcentagem_adicional,
                     x.id_caixa,
                     to_char (c.dt_envio, ''DD/MM/yyyy''),
                     e.id_solicitacao,
                     to_char (s.dt_solicitacao, ''DD/MM/yyyy''),
                     to_char (s.dt_conclusao, ''DD/MM/yyyy'')';
        COMMIT;
    END LOOP;
END;



______________________________


WITH
doc
AS
(SELECT  id_cliente,
cnpj_cpf,
caixa_cliente,
divisao_depto,
departamento,
tipo_de_caixa,
id_documento,
id_documento_sup,
--COLOCA AQUI OS 3 PRIMEIROS INDEXADORES
AGEN_1
,AGENCIA_1
,AGENCIA_PRODUTORA
,data_criacao
,data_destruicao
--COLOCA AQUI OS OUTROS INDEXADORES
,ANO_DO_LIVRO
,ASSUNTO
,BCO
,BORDERO
,CARTA_REMESSA
,CARTEIRA
,CHAVE_UNICA
,CLIENTE
,COD_BANCO
,COD_EMPRESA
,COD_SEGMENTO_DA_AGENCIA_PRODUTORA
,COD_TIPO_DOCUMENTO
,CODIGO_DO_PRODUTO
,CODIGO_DO_SISTEMA
,CODIGO_DOCUMENTO
,CONTA
,CONTA_DE_DEPOSITO
,CONTA_FINAL
,CONTA_INICIAL
,CONTROLE_DE_ETIQUETAS_ANTIGO
,CONTROLE_WANG
,COPIA
,CORREIO
,CPE
,CPF_1
,CPF_CNPJ_1
,DAC
,DATA_1
,DATA_DE_ATENDIMENTO
,DATA_DE_VENCIMENTO
,DATA_EMISSAO_1
,DATA_EMISSAO_2
,DATA_MOV
,DATA_MOVIMENTO
,DATA_OFICIO
,DATA_RECEPCAO
,DATA_RESPOSTA
,DESCRICAO_DO_PRODUTO
,DESCRICAO_TIPO_DOCUMENTO
,DESTINATARIO
,DOCUMENTO_1
,DT_DOCUMENTO_1
,DT_DOCUMENTO_2
,ETIQUETA
,FILIAL
,ID_1
,LACRE
,LOCALIZADOR
,LOTE
,N_CAIXA
,NOME
,NOME_DO_CLIENTE
,NOME_SERVIDOR
,NUMERO
,NUMERO_CAIXA_CLIENTE
,NUMERO_DA_CARTA_REMESSA
,NUMERO_DE_PASTA
,NUMERO_DO_CONTRATO
,OBSERVACAO
,OPERACAO
,ORGAO
,PAG
,PRODUTO
,RAZAO_SOCIAL
,SEGMENTO_DA_AGENCIA_PRODUTORA
,SEQ
,SITUACAO
,STATUS
,SUB_CATEGORIA
,TEXTO
,TIPO_DE_CARTA_REMESSA
,TIPO_DE_PESSOA
,TIPO_DE_PROCESSO
,VALOR
,VALOR_DEVOLVIDO
,VALOR_SOLICITADO
FROM (SELECT p.id_pessoa
id_cliente,
p.cnpj_cpf,
c.id_caixa
caixa_cliente,
pc.descricao
divisao_depto,
pd.descricao
departamento,
(SELECT t.descricao
FROM tipo_caixa t, caixa cc
WHERE cc.id_caixa = c.id_caixa
AND t.id_tipo_caixa = cc.id_tipo_caixa)
tipo_de_caixa,
c.id_documento,
c.id_documento_sup,
NULL
descricao_arquivo_2,
NULL
descricao_arquivo_3,
NULL
descricao_arquivo_4,
to_char(c.dt_expurgo, 'dd/mm/yyyy')
data_destruicao,
to_char(c.dt_cadastro, 'dd/mm/yyyy')
data_criacao,
NULL
descricao_arquivo_5,
NULL
descricao_arquivo_6,
NULL
descricao_arquivo_7,
NULL
descricao_arquivo_8,
NULL
descricao_arquivo_9,
dc.conteudo,
dc.id_criterio_pesquisa
FROM documento c,
pessoa_centro_de_custo pc,
pessoa_departamento pd,
pessoa p,
documento_criterio_pesquisa dc
WHERE c.id_depto = pd.id_depto
AND pd.id_centro_de_custo = pc.id_centro_de_custo
AND c.id_pessoa = p.id_pessoa
AND c.id_pessoa =:id_empresa
AND dc.id_documento(+) = c.id_documento
AND p.id_status = 201
AND c.id_status NOT IN (1503,
1504,
1505,
1506,
1507)
AND c.id_caixa IN
(SELECT id_caixa
FROM caixa ca, armazem_localizacao al
WHERE p.id_pessoa =c.id_pessoa
AND ca.id_status NOT IN
(1305, 1306, 1307)
AND ca.id_local = al.id_local
AND id_armazem = 40)
)
PIVOT (max (conteudo)
FOR (id_criterio_pesquisa)
IN (	
--COLOCA OS INDEXADORES
5149	AGEN_1
,371	AGENCIA_1
,1946	AGENCIA_PRODUTORA
,4291	ANO_DO_LIVRO
,738	ASSUNTO
,5153	BCO
,759	BORDERO
,557	CARTA_REMESSA
,4362	CARTEIRA
,1955	CHAVE_UNICA
,102	CLIENTE
,1945	COD_BANCO
,1944	COD_EMPRESA
,1952	COD_SEGMENTO_DA_AGENCIA_PRODUTORA
,1949	COD_TIPO_DOCUMENTO
,1301	CODIGO_DO_PRODUTO
,1947	CODIGO_DO_SISTEMA
,363	CODIGO_DOCUMENTO
,372	CONTA
,5150	CONTA_DE_DEPOSITO
,355	CONTA_FINAL
,354	CONTA_INICIAL
,4383	CONTROLE_DE_ETIQUETAS_ANTIGO
,4384	CONTROLE_WANG
,556	COPIA
,4394	CORREIO
,5152	CPE
,30		CPF_1
,315	CPF_CNPJ_1
,373	DAC
,166	DATA_1
,2218	DATA_DE_ATENDIMENTO
,351	DATA_DE_VENCIMENTO
,5009	DATA_EMISSAO_1
,753	DATA_EMISSAO_2
,5156	DATA_MOV
,358	DATA_MOVIMENTO
,4387	DATA_OFICIO
,356	DATA_RECEPCAO
,4395	DATA_RESPOSTA
,1304	DESCRICAO_DO_PRODUTO
,1950	DESCRICAO_TIPO_DOCUMENTO
,1980	DESTINATARIO
,565	DOCUMENTO_1
,603	DT_DOCUMENTO_1
,431	DT_DOCUMENTO_2
,292	ETIQUETA
,405	FILIAL
,531	ID_1
,779	LACRE
,376	LOCALIZADOR
,291	LOTE
,810	N_CAIXA
,1733	NOME
,1797	NOME_DO_CLIENTE
,4388	NOME_SERVIDOR
,932	NUMERO
,117	NUMERO_CAIXA_CLIENTE
,1954	NUMERO_DA_CARTA_REMESSA
,4415	NUMERO_DE_PASTA
,1309	NUMERO_DO_CONTRATO
,434	OBSERVACAO
,559	OPERACAO
,4389	ORGAO
,4396	PAG
,1288	PRODUTO
,221	RAZAO_SOCIAL
,1953	SEGMENTO_DA_AGENCIA_PRODUTORA
,4497	SEQ
,1594	SITUACAO
,530	STATUS
,558	SUB_CATEGORIA
,4386	TEXTO
,1948	TIPO_DE_CARTA_REMESSA
,1534	TIPO_DE_PESSOA
,4413	TIPO_DE_PROCESSO
,274	VALOR
,4392	VALOR_DEVOLVIDO
,4391	VALOR_SOLICITADO
))),
PAI as (
SELECT id_documento,
id_documento_sup,
--COLOCA TODOS OS INDEXADORES
AGEN_1
,AGENCIA_1
,AGENCIA_PRODUTORA
,ANO_DO_LIVRO
,ASSUNTO
,BCO
,BORDERO
,CARTA_REMESSA
,CARTEIRA
,CHAVE_UNICA
,CLIENTE
,COD_BANCO
,COD_EMPRESA
,COD_SEGMENTO_DA_AGENCIA_PRODUTORA
,COD_TIPO_DOCUMENTO
,CODIGO_DO_PRODUTO
,CODIGO_DO_SISTEMA
,CODIGO_DOCUMENTO
,CONTA
,CONTA_DE_DEPOSITO
,CONTA_FINAL
,CONTA_INICIAL
,CONTROLE_DE_ETIQUETAS_ANTIGO
,CONTROLE_WANG
,COPIA
,CORREIO
,CPE
,CPF_1
,CPF_CNPJ_1
,DAC
,DATA_1
,DATA_DE_ATENDIMENTO
,DATA_DE_VENCIMENTO
,DATA_EMISSAO_1
,DATA_EMISSAO_2
,DATA_MOV
,DATA_MOVIMENTO
,DATA_OFICIO
,DATA_RECEPCAO
,DATA_RESPOSTA
,DESCRICAO_DO_PRODUTO
,DESCRICAO_TIPO_DOCUMENTO
,DESTINATARIO
,DOCUMENTO_1
,DT_DOCUMENTO_1
,DT_DOCUMENTO_2
,ETIQUETA
,FILIAL
,ID_1
,LACRE
,LOCALIZADOR
,LOTE
,N_CAIXA
,NOME
,NOME_DO_CLIENTE
,NOME_SERVIDOR
,NUMERO
,NUMERO_CAIXA_CLIENTE
,NUMERO_DA_CARTA_REMESSA
,NUMERO_DE_PASTA
,NUMERO_DO_CONTRATO
,OBSERVACAO
,OPERACAO
,ORGAO
,PAG
,PRODUTO
,RAZAO_SOCIAL
,SEGMENTO_DA_AGENCIA_PRODUTORA
,SEQ
,SITUACAO
,STATUS
,SUB_CATEGORIA
,TEXTO
,TIPO_DE_CARTA_REMESSA
,TIPO_DE_PESSOA
,TIPO_DE_PROCESSO
,VALOR
,VALOR_DEVOLVIDO
,VALOR_SOLICITADO
FROM (SELECT c.id_documento,
c.id_documento_sup,
dc.conteudo,
dc.id_criterio_pesquisa
FROM documento c,
pessoa p,
documento_criterio_pesquisa dc
WHERE c.id_pessoa = p.id_pessoa
AND c.id_pessoa = :id_empresa
AND dc.id_documento(+) = c.id_documento
AND c.id_documento_sup is null)
PIVOT (max (conteudo)
FOR (id_criterio_pesquisa)
IN (
--COLOCA OS INDEXADORES COM ID_CRITERIO_PESQUISA
5149	AGEN_1
,371	AGENCIA_1
,1946	AGENCIA_PRODUTORA
,4291	ANO_DO_LIVRO
,738	ASSUNTO
,5153	BCO
,759	BORDERO
,557	CARTA_REMESSA
,4362	CARTEIRA
,1955	CHAVE_UNICA
,102	CLIENTE
,1945	COD_BANCO
,1944	COD_EMPRESA
,1952	COD_SEGMENTO_DA_AGENCIA_PRODUTORA
,1949	COD_TIPO_DOCUMENTO
,1301	CODIGO_DO_PRODUTO
,1947	CODIGO_DO_SISTEMA
,363	CODIGO_DOCUMENTO
,372	CONTA
,5150	CONTA_DE_DEPOSITO
,355	CONTA_FINAL
,354	CONTA_INICIAL
,4383	CONTROLE_DE_ETIQUETAS_ANTIGO
,4384	CONTROLE_WANG
,556	COPIA
,4394	CORREIO
,5152	CPE
,30		CPF_1
,315	CPF_CNPJ_1
,373	DAC
,166	DATA_1
,2218	DATA_DE_ATENDIMENTO
,351	DATA_DE_VENCIMENTO
,5009	DATA_EMISSAO_1
,753	DATA_EMISSAO_2
,5156	DATA_MOV
,358	DATA_MOVIMENTO
,4387	DATA_OFICIO
,356	DATA_RECEPCAO
,4395	DATA_RESPOSTA
,1304	DESCRICAO_DO_PRODUTO
,1950	DESCRICAO_TIPO_DOCUMENTO
,1980	DESTINATARIO
,565	DOCUMENTO_1
,603	DT_DOCUMENTO_1
,431	DT_DOCUMENTO_2
,292	ETIQUETA
,405	FILIAL
,531	ID_1
,779	LACRE
,376	LOCALIZADOR
,291	LOTE
,810	N_CAIXA
,1733	NOME
,1797	NOME_DO_CLIENTE
,4388	NOME_SERVIDOR
,932	NUMERO
,117	NUMERO_CAIXA_CLIENTE
,1954	NUMERO_DA_CARTA_REMESSA
,4415	NUMERO_DE_PASTA
,1309	NUMERO_DO_CONTRATO
,434	OBSERVACAO
,559	OPERACAO
,4389	ORGAO
,4396	PAG
,1288	PRODUTO
,221	RAZAO_SOCIAL
,1953	SEGMENTO_DA_AGENCIA_PRODUTORA
,4497	SEQ
,1594	SITUACAO
,530	STATUS
,558	SUB_CATEGORIA
,4386	TEXTO
,1948	TIPO_DE_CARTA_REMESSA
,1534	TIPO_DE_PESSOA
,4413	TIPO_DE_PROCESSO
,274	VALOR
,4392	VALOR_DEVOLVIDO
,4391	VALOR_SOLICITADO
))
)
SELECT dd.id_cliente,
dd.cnpj_cpf,
dd.caixa_cliente,
dd.divisao_depto,
dd.departamento,
dd.tipo_de_caixa,
dd.id_documento,
dd.id_documento_sup,

--COLOCA OS 3 PRIMEIROS INDEXADORES SUBSTITUINDO
CASE
WHEN dd.id_documento_sup IS NOT NULL
AND dd.AGEN_1 IS NULL
THEN
(SELECT AGEN_1
FROM pai p
WHERE p.id_documento = dd.id_documento_sup)
ELSE
dd.AGEN_1
END
AGEN_1,


CASE
WHEN dd.id_documento_sup IS NOT NULL
AND dd.AGENCIA_1 IS NULL
THEN
(SELECT AGENCIA_1
FROM pai p
WHERE p.id_documento = dd.id_documento_sup)
ELSE
dd.AGENCIA_1
END
AGENCIA_1,


CASE
WHEN dd.id_documento_sup IS NOT NULL
AND dd.AGENCIA_PRODUTORA IS NULL
THEN
(SELECT AGENCIA_PRODUTORA
FROM pai p
WHERE p.id_documento = dd.id_documento_sup)
ELSE
dd.AGENCIA_PRODUTORA
END
AGENCIA_PRODUTORA,

dd.data_destruicao,
dd.data_criacao,

CASE
WHEN dd.id_documento_sup IS NOT NULL
AND dd.ANO_DO_LIVRO IS NULL
THEN
(SELECT ANO_DO_LIVRO
FROM pai p
WHERE p.id_documento = dd.id_documento_sup)
ELSE
dd.ANO_DO_LIVRO
END
ANO_DO_LIVRO,

CASE
WHEN dd.id_documento_sup IS NOT NULL
AND dd.ASSUNTO IS NULL
THEN
(SELECT ASSUNTO
FROM pai p
WHERE p.id_documento = dd.id_documento_sup)
ELSE
dd.ASSUNTO
END
ASSUNTO,

CASE
WHEN dd.id_documento_sup IS NOT NULL
AND dd.BCO IS NULL
THEN
(SELECT BCO
FROM pai p
WHERE p.id_documento = dd.id_documento_sup)
ELSE
dd.BCO
END
BCO,

CASE
WHEN dd.id_documento_sup IS NOT NULL
AND dd.BORDERO IS NULL
THEN
(SELECT BORDERO
FROM pai p
WHERE p.id_documento = dd.id_documento_sup)
ELSE
dd.BORDERO
END
BORDERO,

CASE
WHEN dd.id_documento_sup IS NOT NULL
AND dd.CARTA_REMESSA IS NULL
THEN
(SELECT CARTA_REMESSA
FROM pai p
WHERE p.id_documento = dd.id_documento_sup)
ELSE
dd.CARTA_REMESSA
END
CARTA_REMESSA,

CASE
WHEN dd.id_documento_sup IS NOT NULL
AND dd.CARTEIRA IS NULL
THEN
(SELECT CARTEIRA
FROM pai p
WHERE p.id_documento = dd.id_documento_sup)
ELSE
dd.CARTEIRA
END
CARTEIRA,

CASE
WHEN dd.id_documento_sup IS NOT NULL
AND dd.CHAVE_UNICA IS NULL
THEN
(SELECT CHAVE_UNICA
FROM pai p
WHERE p.id_documento = dd.id_documento_sup)
ELSE
dd.CHAVE_UNICA
END
CHAVE_UNICA,

CASE
WHEN dd.id_documento_sup IS NOT NULL
AND dd.CLIENTE IS NULL
THEN
(SELECT CLIENTE
FROM pai p
WHERE p.id_documento = dd.id_documento_sup)
ELSE
dd.CLIENTE
END
CLIENTE,

CASE
WHEN dd.id_documento_sup IS NOT NULL
AND dd.COD_BANCO IS NULL
THEN
(SELECT COD_BANCO
FROM pai p
WHERE p.id_documento = dd.id_documento_sup)
ELSE
dd.COD_BANCO
END
COD_BANCO,

CASE
WHEN dd.id_documento_sup IS NOT NULL
AND dd.COD_EMPRESA IS NULL
THEN
(SELECT COD_EMPRESA
FROM pai p
WHERE p.id_documento = dd.id_documento_sup)
ELSE
dd.COD_EMPRESA
END
COD_EMPRESA,

CASE
WHEN dd.id_documento_sup IS NOT NULL
AND dd.COD_SEGMENTO_DA_AGENCIA_PRODUTORA IS NULL
THEN
(SELECT COD_SEGMENTO_DA_AGENCIA_PRODUTORA
FROM pai p
WHERE p.id_documento = dd.id_documento_sup)
ELSE
dd.COD_SEGMENTO_DA_AGENCIA_PRODUTORA
END
COD_SEGMENTO_DA_AGENCIA_PRODUTORA,

CASE
WHEN dd.id_documento_sup IS NOT NULL
AND dd.COD_TIPO_DOCUMENTO IS NULL
THEN
(SELECT COD_TIPO_DOCUMENTO
FROM pai p
WHERE p.id_documento = dd.id_documento_sup)
ELSE
dd.COD_TIPO_DOCUMENTO
END
COD_TIPO_DOCUMENTO,

CASE
WHEN dd.id_documento_sup IS NOT NULL
AND dd.CODIGO_DO_PRODUTO IS NULL
THEN
(SELECT CODIGO_DO_PRODUTO
FROM pai p
WHERE p.id_documento = dd.id_documento_sup)
ELSE
dd.CODIGO_DO_PRODUTO
END
CODIGO_DO_PRODUTO,

CASE
WHEN dd.id_documento_sup IS NOT NULL
AND dd.CODIGO_DO_SISTEMA IS NULL
THEN
(SELECT CODIGO_DO_SISTEMA
FROM pai p
WHERE p.id_documento = dd.id_documento_sup)
ELSE
dd.CODIGO_DO_SISTEMA
END
CODIGO_DO_SISTEMA,

CASE
WHEN dd.id_documento_sup IS NOT NULL
AND dd.CODIGO_DOCUMENTO IS NULL
THEN
(SELECT CODIGO_DOCUMENTO
FROM pai p
WHERE p.id_documento = dd.id_documento_sup)
ELSE
dd.CODIGO_DOCUMENTO
END
CODIGO_DOCUMENTO,

CASE
WHEN dd.id_documento_sup IS NOT NULL
AND dd.CONTA IS NULL
THEN
(SELECT CONTA
FROM pai p
WHERE p.id_documento = dd.id_documento_sup)
ELSE
dd.CONTA
END
CONTA,

CASE
WHEN dd.id_documento_sup IS NOT NULL
AND dd.CONTA_DE_DEPOSITO IS NULL
THEN
(SELECT CONTA_DE_DEPOSITO
FROM pai p
WHERE p.id_documento = dd.id_documento_sup)
ELSE
dd.CONTA_DE_DEPOSITO
END
CONTA_DE_DEPOSITO,

CASE
WHEN dd.id_documento_sup IS NOT NULL
AND dd.CONTA_FINAL IS NULL
THEN
(SELECT CONTA_FINAL
FROM pai p
WHERE p.id_documento = dd.id_documento_sup)
ELSE
dd.CONTA_FINAL
END
CONTA_FINAL,

CASE
WHEN dd.id_documento_sup IS NOT NULL
AND dd.CONTA_INICIAL IS NULL
THEN
(SELECT CONTA_INICIAL
FROM pai p
WHERE p.id_documento = dd.id_documento_sup)
ELSE
dd.CONTA_INICIAL
END
CONTA_INICIAL,

CASE
WHEN dd.id_documento_sup IS NOT NULL
AND dd.CONTROLE_DE_ETIQUETAS_ANTIGO IS NULL
THEN
(SELECT CONTROLE_DE_ETIQUETAS_ANTIGO
FROM pai p
WHERE p.id_documento = dd.id_documento_sup)
ELSE
dd.CONTROLE_DE_ETIQUETAS_ANTIGO
END
CONTROLE_DE_ETIQUETAS_ANTIGO,

CASE
WHEN dd.id_documento_sup IS NOT NULL
AND dd.CONTROLE_WANG IS NULL
THEN
(SELECT CONTROLE_WANG
FROM pai p
WHERE p.id_documento = dd.id_documento_sup)
ELSE
dd.CONTROLE_WANG
END
CONTROLE_WANG,

CASE
WHEN dd.id_documento_sup IS NOT NULL
AND dd.COPIA IS NULL
THEN
(SELECT COPIA
FROM pai p
WHERE p.id_documento = dd.id_documento_sup)
ELSE
dd.COPIA
END
COPIA,

CASE
WHEN dd.id_documento_sup IS NOT NULL
AND dd.CORREIO IS NULL
THEN
(SELECT CORREIO
FROM pai p
WHERE p.id_documento = dd.id_documento_sup)
ELSE
dd.CORREIO
END
CORREIO,

CASE
WHEN dd.id_documento_sup IS NOT NULL
AND dd.CPE IS NULL
THEN
(SELECT CPE
FROM pai p
WHERE p.id_documento = dd.id_documento_sup)
ELSE
dd.CPE
END
CPE,

CASE
WHEN dd.id_documento_sup IS NOT NULL
AND dd.CPF_1 IS NULL
THEN
(SELECT CPF_1
FROM pai p
WHERE p.id_documento = dd.id_documento_sup)
ELSE
dd.CPF_1
END
CPF_1,

CASE
WHEN dd.id_documento_sup IS NOT NULL
AND dd.CPF_CNPJ_1 IS NULL
THEN
(SELECT CPF_CNPJ_1
FROM pai p
WHERE p.id_documento = dd.id_documento_sup)
ELSE
dd.CPF_CNPJ_1
END
CPF_CNPJ_1,

CASE
WHEN dd.id_documento_sup IS NOT NULL
AND dd.DAC IS NULL
THEN
(SELECT DAC
FROM pai p
WHERE p.id_documento = dd.id_documento_sup)
ELSE
dd.DAC
END
DAC,

CASE
WHEN dd.id_documento_sup IS NOT NULL
AND dd.DATA_1 IS NULL
THEN
(SELECT DATA_1
FROM pai p
WHERE p.id_documento = dd.id_documento_sup)
ELSE
dd.DATA_1
END
DATA_1,

CASE
WHEN dd.id_documento_sup IS NOT NULL
AND dd.DATA_DE_ATENDIMENTO IS NULL
THEN
(SELECT DATA_DE_ATENDIMENTO
FROM pai p
WHERE p.id_documento = dd.id_documento_sup)
ELSE
dd.DATA_DE_ATENDIMENTO
END
DATA_DE_ATENDIMENTO,

CASE
WHEN dd.id_documento_sup IS NOT NULL
AND dd.DATA_DE_VENCIMENTO IS NULL
THEN
(SELECT DATA_DE_VENCIMENTO
FROM pai p
WHERE p.id_documento = dd.id_documento_sup)
ELSE
dd.DATA_DE_VENCIMENTO
END
DATA_DE_VENCIMENTO,

CASE
WHEN dd.id_documento_sup IS NOT NULL
AND dd.DATA_EMISSAO_1 IS NULL
THEN
(SELECT DATA_EMISSAO_1
FROM pai p
WHERE p.id_documento = dd.id_documento_sup)
ELSE
dd.DATA_EMISSAO_1
END
DATA_EMISSAO_1,

CASE
WHEN dd.id_documento_sup IS NOT NULL
AND dd.DATA_EMISSAO_2 IS NULL
THEN
(SELECT DATA_EMISSAO_2
FROM pai p
WHERE p.id_documento = dd.id_documento_sup)
ELSE
dd.DATA_EMISSAO_2
END
DATA_EMISSAO_2,

CASE
WHEN dd.id_documento_sup IS NOT NULL
AND dd.DATA_MOV IS NULL
THEN
(SELECT DATA_MOV
FROM pai p
WHERE p.id_documento = dd.id_documento_sup)
ELSE
dd.DATA_MOV
END
DATA_MOV,

CASE
WHEN dd.id_documento_sup IS NOT NULL
AND dd.DATA_MOVIMENTO IS NULL
THEN
(SELECT DATA_MOVIMENTO
FROM pai p
WHERE p.id_documento = dd.id_documento_sup)
ELSE
dd.DATA_MOVIMENTO
END
DATA_MOVIMENTO,

CASE
WHEN dd.id_documento_sup IS NOT NULL
AND dd.DATA_OFICIO IS NULL
THEN
(SELECT DATA_OFICIO
FROM pai p
WHERE p.id_documento = dd.id_documento_sup)
ELSE
dd.DATA_OFICIO
END
DATA_OFICIO,

CASE
WHEN dd.id_documento_sup IS NOT NULL
AND dd.DATA_RECEPCAO IS NULL
THEN
(SELECT DATA_RECEPCAO
FROM pai p
WHERE p.id_documento = dd.id_documento_sup)
ELSE
dd.DATA_RECEPCAO
END
DATA_RECEPCAO,

CASE
WHEN dd.id_documento_sup IS NOT NULL
AND dd.DATA_RESPOSTA IS NULL
THEN
(SELECT DATA_RESPOSTA
FROM pai p
WHERE p.id_documento = dd.id_documento_sup)
ELSE
dd.DATA_RESPOSTA
END
DATA_RESPOSTA,

CASE
WHEN dd.id_documento_sup IS NOT NULL
AND dd.DESCRICAO_DO_PRODUTO IS NULL
THEN
(SELECT DESCRICAO_DO_PRODUTO
FROM pai p
WHERE p.id_documento = dd.id_documento_sup)
ELSE
dd.DESCRICAO_DO_PRODUTO
END
DESCRICAO_DO_PRODUTO,

CASE
WHEN dd.id_documento_sup IS NOT NULL
AND dd.DESCRICAO_TIPO_DOCUMENTO IS NULL
THEN
(SELECT DESCRICAO_TIPO_DOCUMENTO
FROM pai p
WHERE p.id_documento = dd.id_documento_sup)
ELSE
dd.DESCRICAO_TIPO_DOCUMENTO
END
DESCRICAO_TIPO_DOCUMENTO,

CASE
WHEN dd.id_documento_sup IS NOT NULL
AND dd.DESTINATARIO IS NULL
THEN
(SELECT DESTINATARIO
FROM pai p
WHERE p.id_documento = dd.id_documento_sup)
ELSE
dd.DESTINATARIO
END
DESTINATARIO,

CASE
WHEN dd.id_documento_sup IS NOT NULL
AND dd.DOCUMENTO_1 IS NULL
THEN
(SELECT DOCUMENTO_1
FROM pai p
WHERE p.id_documento = dd.id_documento_sup)
ELSE
dd.DOCUMENTO_1
END
DOCUMENTO_1,

CASE
WHEN dd.id_documento_sup IS NOT NULL
AND dd.DT_DOCUMENTO_1 IS NULL
THEN
(SELECT DT_DOCUMENTO_1
FROM pai p
WHERE p.id_documento = dd.id_documento_sup)
ELSE
dd.DT_DOCUMENTO_1
END
DT_DOCUMENTO_1,

CASE
WHEN dd.id_documento_sup IS NOT NULL
AND dd.DT_DOCUMENTO_2 IS NULL
THEN
(SELECT DT_DOCUMENTO_2
FROM pai p
WHERE p.id_documento = dd.id_documento_sup)
ELSE
dd.DT_DOCUMENTO_2
END
DT_DOCUMENTO_2,

CASE
WHEN dd.id_documento_sup IS NOT NULL
AND dd.ETIQUETA IS NULL
THEN
(SELECT ETIQUETA
FROM pai p
WHERE p.id_documento = dd.id_documento_sup)
ELSE
dd.ETIQUETA
END
ETIQUETA,

CASE
WHEN dd.id_documento_sup IS NOT NULL
AND dd.FILIAL IS NULL
THEN
(SELECT FILIAL
FROM pai p
WHERE p.id_documento = dd.id_documento_sup)
ELSE
dd.FILIAL
END
FILIAL,

CASE
WHEN dd.id_documento_sup IS NOT NULL
AND dd.ID_1 IS NULL
THEN
(SELECT ID_1
FROM pai p
WHERE p.id_documento = dd.id_documento_sup)
ELSE
dd.ID_1
END
ID_1,

CASE
WHEN dd.id_documento_sup IS NOT NULL
AND dd.LACRE IS NULL
THEN
(SELECT LACRE
FROM pai p
WHERE p.id_documento = dd.id_documento_sup)
ELSE
dd.LACRE
END
LACRE,

CASE
WHEN dd.id_documento_sup IS NOT NULL
AND dd.LOCALIZADOR IS NULL
THEN
(SELECT LOCALIZADOR
FROM pai p
WHERE p.id_documento = dd.id_documento_sup)
ELSE
dd.LOCALIZADOR
END
LOCALIZADOR,

CASE
WHEN dd.id_documento_sup IS NOT NULL
AND dd.LOTE IS NULL
THEN
(SELECT LOTE
FROM pai p
WHERE p.id_documento = dd.id_documento_sup)
ELSE
dd.LOTE
END
LOTE,

CASE
WHEN dd.id_documento_sup IS NOT NULL
AND dd.N_CAIXA IS NULL
THEN
(SELECT N_CAIXA
FROM pai p
WHERE p.id_documento = dd.id_documento_sup)
ELSE
dd.N_CAIXA
END
N_CAIXA,

CASE
WHEN dd.id_documento_sup IS NOT NULL
AND dd.NOME IS NULL
THEN
(SELECT NOME
FROM pai p
WHERE p.id_documento = dd.id_documento_sup)
ELSE
dd.NOME
END
NOME,

CASE
WHEN dd.id_documento_sup IS NOT NULL
AND dd.NOME_DO_CLIENTE IS NULL
THEN
(SELECT NOME_DO_CLIENTE
FROM pai p
WHERE p.id_documento = dd.id_documento_sup)
ELSE
dd.NOME_DO_CLIENTE
END
NOME_DO_CLIENTE,

CASE
WHEN dd.id_documento_sup IS NOT NULL
AND dd.NOME_SERVIDOR IS NULL
THEN
(SELECT NOME_SERVIDOR
FROM pai p
WHERE p.id_documento = dd.id_documento_sup)
ELSE
dd.NOME_SERVIDOR
END
NOME_SERVIDOR,

CASE
WHEN dd.id_documento_sup IS NOT NULL
AND dd.NUMERO IS NULL
THEN
(SELECT NUMERO
FROM pai p
WHERE p.id_documento = dd.id_documento_sup)
ELSE
dd.NUMERO
END
NUMERO,

CASE
WHEN dd.id_documento_sup IS NOT NULL
AND dd.NUMERO_CAIXA_CLIENTE IS NULL
THEN
(SELECT NUMERO_CAIXA_CLIENTE
FROM pai p
WHERE p.id_documento = dd.id_documento_sup)
ELSE
dd.NUMERO_CAIXA_CLIENTE
END
NUMERO_CAIXA_CLIENTE,

CASE
WHEN dd.id_documento_sup IS NOT NULL
AND dd.NUMERO_DA_CARTA_REMESSA IS NULL
THEN
(SELECT NUMERO_DA_CARTA_REMESSA
FROM pai p
WHERE p.id_documento = dd.id_documento_sup)
ELSE
dd.NUMERO_DA_CARTA_REMESSA
END
NUMERO_DA_CARTA_REMESSA,

CASE
WHEN dd.id_documento_sup IS NOT NULL
AND dd.NUMERO_DE_PASTA IS NULL
THEN
(SELECT NUMERO_DE_PASTA
FROM pai p
WHERE p.id_documento = dd.id_documento_sup)
ELSE
dd.NUMERO_DE_PASTA
END
NUMERO_DE_PASTA,

CASE
WHEN dd.id_documento_sup IS NOT NULL
AND dd.NUMERO_DO_CONTRATO IS NULL
THEN
(SELECT NUMERO_DO_CONTRATO
FROM pai p
WHERE p.id_documento = dd.id_documento_sup)
ELSE
dd.NUMERO_DO_CONTRATO
END
NUMERO_DO_CONTRATO,

CASE
WHEN dd.id_documento_sup IS NOT NULL
AND dd.OBSERVACAO IS NULL
THEN
(SELECT OBSERVACAO
FROM pai p
WHERE p.id_documento = dd.id_documento_sup)
ELSE
dd.OBSERVACAO
END
OBSERVACAO,

CASE
WHEN dd.id_documento_sup IS NOT NULL
AND dd.OPERACAO IS NULL
THEN
(SELECT OPERACAO
FROM pai p
WHERE p.id_documento = dd.id_documento_sup)
ELSE
dd.OPERACAO
END
OPERACAO,

CASE
WHEN dd.id_documento_sup IS NOT NULL
AND dd.ORGAO IS NULL
THEN
(SELECT ORGAO
FROM pai p
WHERE p.id_documento = dd.id_documento_sup)
ELSE
dd.ORGAO
END
ORGAO,

CASE
WHEN dd.id_documento_sup IS NOT NULL
AND dd.PAG IS NULL
THEN
(SELECT PAG
FROM pai p
WHERE p.id_documento = dd.id_documento_sup)
ELSE
dd.PAG
END
PAG,

CASE
WHEN dd.id_documento_sup IS NOT NULL
AND dd.PRODUTO IS NULL
THEN
(SELECT PRODUTO
FROM pai p
WHERE p.id_documento = dd.id_documento_sup)
ELSE
dd.PRODUTO
END
PRODUTO,

CASE
WHEN dd.id_documento_sup IS NOT NULL
AND dd.RAZAO_SOCIAL IS NULL
THEN
(SELECT RAZAO_SOCIAL
FROM pai p
WHERE p.id_documento = dd.id_documento_sup)
ELSE
dd.RAZAO_SOCIAL
END
RAZAO_SOCIAL,

CASE
WHEN dd.id_documento_sup IS NOT NULL
AND dd.SEGMENTO_DA_AGENCIA_PRODUTORA IS NULL
THEN
(SELECT SEGMENTO_DA_AGENCIA_PRODUTORA
FROM pai p
WHERE p.id_documento = dd.id_documento_sup)
ELSE
dd.SEGMENTO_DA_AGENCIA_PRODUTORA
END
SEGMENTO_DA_AGENCIA_PRODUTORA,

CASE
WHEN dd.id_documento_sup IS NOT NULL
AND dd.SEQ IS NULL
THEN
(SELECT SEQ
FROM pai p
WHERE p.id_documento = dd.id_documento_sup)
ELSE
dd.SEQ
END
SEQ,

CASE
WHEN dd.id_documento_sup IS NOT NULL
AND dd.SITUACAO IS NULL
THEN
(SELECT SITUACAO
FROM pai p
WHERE p.id_documento = dd.id_documento_sup)
ELSE
dd.SITUACAO
END
SITUACAO,

CASE
WHEN dd.id_documento_sup IS NOT NULL
AND dd.STATUS IS NULL
THEN
(SELECT STATUS
FROM pai p
WHERE p.id_documento = dd.id_documento_sup)
ELSE
dd.STATUS
END
STATUS,

CASE
WHEN dd.id_documento_sup IS NOT NULL
AND dd.SUB_CATEGORIA IS NULL
THEN
(SELECT SUB_CATEGORIA
FROM pai p
WHERE p.id_documento = dd.id_documento_sup)
ELSE
dd.SUB_CATEGORIA
END
SUB_CATEGORIA,

CASE
WHEN dd.id_documento_sup IS NOT NULL
AND dd.TEXTO IS NULL
THEN
(SELECT TEXTO
FROM pai p
WHERE p.id_documento = dd.id_documento_sup)
ELSE
dd.INDEXADOR_6
END
INDEXADOR_6,

CASE
WHEN dd.id_documento_sup IS NOT NULL
AND dd.INDEXADOR_6 IS NULL
THEN
(SELECT INDEXADOR_6
FROM pai p
WHERE p.id_documento = dd.id_documento_sup)
ELSE
dd.INDEXADOR_6
END
INDEXADOR_6,

CASE
WHEN dd.id_documento_sup IS NOT NULL
AND dd.INDEXADOR_6 IS NULL
THEN
(SELECT INDEXADOR_6
FROM pai p
WHERE p.id_documento = dd.id_documento_sup)
ELSE
dd.INDEXADOR_6
END
INDEXADOR_6,

CASE
WHEN dd.id_documento_sup IS NOT NULL
AND dd.INDEXADOR_6 IS NULL
THEN
(SELECT INDEXADOR_6
FROM pai p
WHERE p.id_documento = dd.id_documento_sup)
ELSE
dd.INDEXADOR_6
END
INDEXADOR_6,

CASE
WHEN dd.id_documento_sup IS NOT NULL
AND dd.INDEXADOR_6 IS NULL
THEN
(SELECT INDEXADOR_6
FROM pai p
WHERE p.id_documento = dd.id_documento_sup)
ELSE
dd.INDEXADOR_6
END
INDEXADOR_6,

CASE
WHEN dd.id_documento_sup IS NOT NULL
AND dd.INDEXADOR_6 IS NULL
THEN
(SELECT INDEXADOR_6
FROM pai p
WHERE p.id_documento = dd.id_documento_sup)
ELSE
dd.INDEXADOR_6
END
INDEXADOR_6,

CASE
WHEN dd.id_documento_sup IS NOT NULL
AND dd.INDEXADOR_6 IS NULL
THEN
(SELECT INDEXADOR_6
FROM pai p
WHERE p.id_documento = dd.id_documento_sup)
ELSE
dd.INDEXADOR_6
END
INDEXADOR_6,

CASE
WHEN dd.id_documento_sup IS NOT NULL
AND dd.INDEXADOR_6 IS NULL
THEN
(SELECT INDEXADOR_6
FROM pai p
WHERE p.id_documento = dd.id_documento_sup)
ELSE
dd.INDEXADOR_6
END
INDEXADOR_6,

CASE
WHEN dd.id_documento_sup IS NOT NULL
AND dd.INDEXADOR_6 IS NULL
THEN
(SELECT INDEXADOR_6
FROM pai p
WHERE p.id_documento = dd.id_documento_sup)
ELSE
dd.INDEXADOR_6
END
INDEXADOR_6,

CASE
WHEN dd.id_documento_sup IS NOT NULL
AND dd.INDEXADOR_6 IS NULL
THEN
(SELECT INDEXADOR_6
FROM pai p
WHERE p.id_documento = dd.id_documento_sup)
ELSE
dd.INDEXADOR_6
END
INDEXADOR_6,

CASE
WHEN dd.id_documento_sup IS NOT NULL
AND dd.INDEXADOR_6 IS NULL
THEN
(SELECT INDEXADOR_6
FROM pai p
WHERE p.id_documento = dd.id_documento_sup)
ELSE
dd.INDEXADOR_6
END
INDEXADOR_6,

CASE
WHEN dd.id_documento_sup IS NOT NULL
AND dd.INDEXADOR_6 IS NULL
THEN
(SELECT INDEXADOR_6
FROM pai p
WHERE p.id_documento = dd.id_documento_sup)
ELSE
dd.INDEXADOR_6
END
INDEXADOR_6,

CASE
WHEN dd.id_documento_sup IS NOT NULL
AND dd.INDEXADOR_6 IS NULL
THEN
(SELECT INDEXADOR_6
FROM pai p
WHERE p.id_documento = dd.id_documento_sup)
ELSE
dd.INDEXADOR_6
END
INDEXADOR_6,

CASE
WHEN dd.id_documento_sup IS NOT NULL
AND dd.INDEXADOR_6 IS NULL
THEN
(SELECT INDEXADOR_6
FROM pai p
WHERE p.id_documento = dd.id_documento_sup)
ELSE
dd.INDEXADOR_6
END
INDEXADOR_6,

CASE
WHEN dd.id_documento_sup IS NOT NULL
AND dd.INDEXADOR_6 IS NULL
THEN
(SELECT INDEXADOR_6
FROM pai p
WHERE p.id_documento = dd.id_documento_sup)
ELSE
dd.INDEXADOR_6
END
INDEXADOR_6,

CASE
WHEN dd.id_documento_sup IS NOT NULL
AND dd.INDEXADOR_6 IS NULL
THEN
(SELECT INDEXADOR_6
FROM pai p
WHERE p.id_documento = dd.id_documento_sup)
ELSE
dd.INDEXADOR_6
END
INDEXADOR_6,

CASE
WHEN dd.id_documento_sup IS NOT NULL
AND dd.INDEXADOR_6 IS NULL
THEN
(SELECT INDEXADOR_6
FROM pai p
WHERE p.id_documento = dd.id_documento_sup)
ELSE
dd.INDEXADOR_6
END
INDEXADOR_6,

CASE
WHEN dd.id_documento_sup IS NOT NULL
AND dd.INDEXADOR_6 IS NULL
THEN
(SELECT INDEXADOR_6
FROM pai p
WHERE p.id_documento = dd.id_documento_sup)
ELSE
dd.INDEXADOR_6
END
INDEXADOR_6,

CASE
WHEN dd.id_documento_sup IS NOT NULL
AND dd.INDEXADOR_6 IS NULL
THEN
(SELECT INDEXADOR_6
FROM pai p
WHERE p.id_documento = dd.id_documento_sup)
ELSE
dd.INDEXADOR_6
END
INDEXADOR_6,

CASE
WHEN dd.id_documento_sup IS NOT NULL
AND dd.INDEXADOR_6 IS NULL
THEN
(SELECT INDEXADOR_6
FROM pai p
WHERE p.id_documento = dd.id_documento_sup)
ELSE
dd.INDEXADOR_6
END
INDEXADOR_6,

CASE
WHEN dd.id_documento_sup IS NOT NULL
AND dd.INDEXADOR_6 IS NULL
THEN
(SELECT INDEXADOR_6
FROM pai p
WHERE p.id_documento = dd.id_documento_sup)
ELSE
dd.INDEXADOR_6
END
INDEXADOR_6,

CASE
WHEN dd.id_documento_sup IS NOT NULL
AND dd.INDEXADOR_6 IS NULL
THEN
(SELECT INDEXADOR_6
FROM pai p
WHERE p.id_documento = dd.id_documento_sup)
ELSE
dd.INDEXADOR_6
END
INDEXADOR_6,

CASE
WHEN dd.id_documento_sup IS NOT NULL
AND dd.INDEXADOR_6 IS NULL
THEN
(SELECT INDEXADOR_6
FROM pai p
WHERE p.id_documento = dd.id_documento_sup)
ELSE
dd.INDEXADOR_6
END
INDEXADOR_6,

CASE
WHEN dd.id_documento_sup IS NOT NULL
AND dd.INDEXADOR_6 IS NULL
THEN
(SELECT INDEXADOR_6
FROM pai p
WHERE p.id_documento = dd.id_documento_sup)
ELSE
dd.INDEXADOR_6
END
INDEXADOR_6,

CASE
WHEN dd.id_documento_sup IS NOT NULL
AND dd.INDEXADOR_6 IS NULL
THEN
(SELECT INDEXADOR_6
FROM pai p
WHERE p.id_documento = dd.id_documento_sup)
ELSE
dd.INDEXADOR_6
END
INDEXADOR_6,

CASE
WHEN dd.id_documento_sup IS NOT NULL
AND dd.INDEXADOR_6 IS NULL
THEN
(SELECT INDEXADOR_6
FROM pai p
WHERE p.id_documento = dd.id_documento_sup)
ELSE
dd.INDEXADOR_6
END
INDEXADOR_6,

CASE
WHEN dd.id_documento_sup IS NOT NULL
AND dd.INDEXADOR_6 IS NULL
THEN
(SELECT INDEXADOR_6
FROM pai p
WHERE p.id_documento = dd.id_documento_sup)
ELSE
dd.INDEXADOR_6
END
INDEXADOR_6,

CASE
WHEN dd.id_documento_sup IS NOT NULL
AND dd.INDEXADOR_6 IS NULL
THEN
(SELECT INDEXADOR_6
FROM pai p
WHERE p.id_documento = dd.id_documento_sup)
ELSE
dd.INDEXADOR_6
END
INDEXADOR_6,

CASE
WHEN dd.id_documento_sup IS NOT NULL
AND dd.INDEXADOR_6 IS NULL
THEN
(SELECT INDEXADOR_6
FROM pai p
WHERE p.id_documento = dd.id_documento_sup)
ELSE
dd.INDEXADOR_6
END
INDEXADOR_6,

CASE
WHEN dd.id_documento_sup IS NOT NULL
AND dd.INDEXADOR_6 IS NULL
THEN
(SELECT INDEXADOR_6
FROM pai p
WHERE p.id_documento = dd.id_documento_sup)
ELSE
dd.INDEXADOR_6
END
INDEXADOR_6,

CASE
WHEN dd.id_documento_sup IS NOT NULL
AND dd.INDEXADOR_6 IS NULL
THEN
(SELECT INDEXADOR_6
FROM pai p
WHERE p.id_documento = dd.id_documento_sup)
ELSE
dd.INDEXADOR_6
END
INDEXADOR_6,

CASE
WHEN dd.id_documento_sup IS NOT NULL
AND dd.INDEXADOR_6 IS NULL
THEN
(SELECT INDEXADOR_6
FROM pai p
WHERE p.id_documento = dd.id_documento_sup)
ELSE
dd.INDEXADOR_6
END
INDEXADOR_6
--VAI TER UM CASE WHEN POR INDEXADOR
--RODA E PASSA O ID_PESSOA

--da um ctrl+END no resultado (caso demore muito faz o proximo direto)
--clica com o botão direito e exportar, delimitado, separador | 
--coloca o nome de DocumentosFantasia.csv

FROM doc dd;

