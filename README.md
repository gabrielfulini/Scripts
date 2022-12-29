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