with selecao as (
select ptrf.id_processo_trf 
	  ,oj.nr_vara as vara_trabalho
	  ,pro.nr_processo
	  ,classe.ds_classe_judicial_sigla as classe_processual
	  ,ta.id_pessoa_magistrado as juiz
	  ,tpa.id_tipo_audiencia as tipo_audiencia
	  ,tpa.dt_inicio as dt_inicio_programada
	  ,tpa.dt_fim as dt_fim_programada
	  ,EXTRACT(EPOCH FROM tpa.dt_fim-tpa.dt_inicio  )/60 as duracao_programada_minutos
	  ,case when tpa.cd_tipo_designacao = 'S' then 'Sugerida' else 'Manual' end as marcacao_automatica 
	  ,translate( trim( substring(substring(ds_conteudo_documento from '.{100}.aberta.+?audi.+?ncia') from '(\d{1,2}[h:\s]\d{1,2})')) ,'h ','::' ) as   hora_abertura
	  ,translate( trim( substring(substring(ds_conteudo_documento from 'Audi.*?ncia encerrada .agrave;s(.*?).?</') from '(\d{1,2}[h:\s]\d{1,2})')) ,'h ','::' ) as   hora_encerramento
	  ,to_char(tpa.dt_inicio,'yyyy-mm-dd') as data_audiencia
	  ,extract(dow from ta.dt_inicio)::integer+1 as dia_semana
	  ,tpa.id_sala_fisica as sala_audiencia
	  ,mibge.id_municipio_ibge as municipio
	  ,case when ptrf.dt_distribuicao >= to_date('11/11/2017','dd/mm/yyyy') then 'Apos Reforma' else 'Antes Reforma' end as apos_reforma_trabalhista
	  ,ptrf.vl_causa as valor_causa
	  ,case when ptrf.in_segredo_justica = 'S' then 'Com Segredo' else 'Sem Segredo' end as segredo_justica
	  ,case when ptrf.in_justica_gratuita = 'S' then 'Gratuita' else 'Nao Gratuita' end as justica_gratuita
	  ,case when ptrf.in_tutela_liminar = 'S' then 'Houve tutela' else 'Sem tutela' end as tutela_liminar
	  ,case when prioridade.id_prioridade_processo is not null then 'Com Prioridade' else 'Sem Prioridade' end as prioridade
--	  ,pjt.id_atividade_economica as atividade_economica
	  ,tae.nome_atividade_economica as atividade_economica
	  ,pai.ds_assunto_trf as assunto
--	  ,atr.id_assunto_trf_superior as assunto_pai
--	  ,pai.ds_assunto_trf as assunto_pai
	  ,ppa.id_pessoa as autor
--	  ,pdia.cd_tp_documento_identificacao as tipo_documento_autor
	  ,ppr.id_pessoa as reu
--	  ,pdir.cd_tp_documento_identificacao as tipo_documento_reu
from tb_aud_importacao ta
join tb_processo_trf ptrf on ptrf.id_processo_trf = ta.id_processo
join tb_processo pro on ta.id_processo=pro.id_processo
join tb_orgao_julgador oj using (id_orgao_julgador)
join tb_classe_judicial classe using (id_classe_judicial)
join tb_processo_audiencia tpa on (tpa.dt_inicio = ta.dt_inicio and tpa.id_processo_trf = ta.id_processo)
left join pje_jt.tb_processo_jt pjt on pjt.id_processo_trf = pro.id_processo
join tb_atividade_economica tae on tae.id_atividade_economica = pjt.id_atividade_economica
left join pje.tb_municipio mun on mun.id_municipio = pjt.id_municipio
left join pje_jt.tb_municipio_ibge mibge on mun.id_municipio = mibge.id_municipio
left join pje.tb_proc_prioridde_processo prioridade on prioridade.id_processo_trf = pro.id_processo
left join pje.tb_processo_assunto pa on pa.id_processo_trf = ptrf.id_processo_trf
join pje.tb_assunto_trf atr on atr.id_assunto_trf = pa.id_assunto_trf
join pje.tb_assunto_trf pai on pai.id_assunto_trf = atr.id_assunto_trf_superior	
join pje.tb_processo_parte ppa on ppa.id_processo_trf = ptrf.id_processo_trf
join pje.tb_usuario_login la on la.id_usuario = ppa.id_pessoa
join pje.tb_pess_doc_identificacao pdia on pdia.id_pessoa = la.id_usuario and pdia.in_principal = 'S' and pdia.cd_tp_documento_identificacao in ('CPF', 'CPJ')
join pje.tb_processo_parte ppr on ppr.id_processo_trf = ptrf.id_processo_trf
join pje.tb_usuario_login lr on lr.id_usuario = ppr.id_pessoa
join pje.tb_pess_doc_identificacao pdir on pdir.id_pessoa = lr.id_usuario and pdir.in_principal = 'S' and pdir.cd_tp_documento_identificacao in ('CPF', 'CPJ')
where extract(year from ta.dt_inicio) in (2018,2019)
and tpa.cd_status_audiencia = 'F' -- realizada
and pa.in_assunto_principal = 'S'
and ppa.in_parte_principal = 'S'
and ppa.in_participacao = 'A'
and ppa.in_situacao = 'A'
and ppr.in_parte_principal = 'S'
and ppr.in_participacao = 'P'
and ppr.in_situacao = 'A'
and ppr.nr_ordem = 1
	)
select s.* , 
DATE_PART('hour', s.hora_encerramento::time - s.hora_abertura::time) * 60 +
              DATE_PART('minute', s.hora_encerramento::time - s.hora_abertura::time) as duracao_audiencia
from selecao s
where hora_encerramento is not null 
and hora_encerramento between '00:00' and '23:59'
and hora_abertura between '00:00' and '23:59'
and substring(hora_encerramento,4) < '60'
and substring(hora_abertura,4) < '60'
