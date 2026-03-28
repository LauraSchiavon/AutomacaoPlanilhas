import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
import sys
import os
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots

# Adicionar o diretório de execução ao path para importar a API do Facebook e Excel Utils
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from facebook_redtrack_importer_v2 import FacebookAdsAPI, ExcelManager, main as run_importer_script
from fill_creative_tests import fill_creative_tests

# Configuração da página Streamlit
st.set_page_config(page_title="Gestor de Performance FB Ads", page_icon="📈", layout="wide")

# Sidebar - Configurações Iniciais
st.sidebar.title("Configurações ⚙️")

# Default token (ocultado do código fonte no production)
st.sidebar.markdown("**Tokens de Integração API**")
fb_token = st.sidebar.text_input("Token do Facebook Ads", type="password")

if not fb_token:
    fb_token = "EAAWDHozjODgBQ6y16hDYtG5psMsSYZBv3S6O8w3lxeihOtCryNgLFwQXGVxwbJuSks9BJQZBsWY2Iw5ZAuV9sCuTmBusecWWrkxb5glHaKLE9DSvhe6edmfT0b96sVWGZAsBVwZCMAF9cKsPJQ4qZBEGJWFoOZA9iDAPZBFT5GNUMRIIJEjj0hik5d1nTZAvWcjNMwMurcmcWgOfXOWtaZBaM3nYZB2EbdqllBH3mAjCxSGkAgocfYvJZBZCkZCdfmqUcY5LekdzG3w6doGIB0IytWhbY9JZBTEepCY5DgS3ZB8OXQZDZD"

rt_token = st.sidebar.text_input("Chave de API RedTrack", type="password")

if not rt_token:
    rt_token = "wB7qY69R0KVU9tl4TBaQ"

st.sidebar.markdown("---")

@st.cache_resource
def get_fb_api(token):
    return FacebookAdsAPI(token)

# Título Principal do App
st.title("Gestor de Performance - Criativos 🚀")
st.markdown("Filtre e visualize a performance das suas campanhas em tempo real diretamente da API do Facebook Ads e RedTrack.")

# Área de parâmetros do filtro
col1, col2, col3 = st.columns(3)

with col1:
    st.markdown("### 1. Data do Relatório")
    date_start = st.date_input("Data Inicial", datetime.now() - timedelta(days=30))
    date_end = st.date_input("Data Final", datetime.now())

with col2:
    st.markdown("### 2. Conta de Anúncios")
    
    # Vamos armazenar cache de instâncias API para performance
    fb_api = get_fb_api(fb_token)
    
    @st.cache_data(ttl=3600, show_spinner=False)
    def fetch_bms(token):
        bms = get_fb_api(token).get_business_managers()
        return bms if bms else None # Retorna None se falhar em vez de [] vazio para evitar cache infinito
        
    bm_list = fetch_bms(fb_token)
    
    if bm_list is None:
         st.error("Token do Facebook inválido ou expirado. Cole um novo token no menu lateral.")
         bm_options = {}
    else:
         bm_options = {f"{bm['name']} ({bm['id']})": bm['id'] for bm in bm_list}
         
    if not bm_options:
        st.selectbox("Selecione o(s) Business Manager(s)", ["Nenhum encontrado"])
        selected_bm_names = []
    else:
        default_bm = list(bm_options.keys())  # Select ALL BMs by default so no account is missed
        selected_bm_names = st.multiselect("Selecione o(s) Business Manager(s)", list(bm_options.keys()), default=default_bm)
    
    selected_bm_ids = [bm_options.get(name) for name in selected_bm_names]
    selected_account_names = []
    selected_account_ids = []

    @st.cache_data(ttl=3600)
    def fetch_accounts(token, bm_id):
         return get_fb_api(token).get_ad_accounts(bm_id)

    if selected_bm_ids:
        with st.spinner('Carregando Ad Accounts...'):
            account_list = []
            for bm_id in selected_bm_ids:
                accounts = fetch_accounts(fb_token, bm_id)
                if accounts:
                    account_list.extend(accounts)
                    
            if account_list:
                account_options = {f"{acc['name']} ({acc['id']})": acc['id'] for acc in account_list}
                default_val = list(account_options.keys())  # Select ALL accounts by default
                selected_account_names = st.multiselect("Selecione a(s) Conta(s) de Anúncios", list(account_options.keys()), default=default_val)
                selected_account_ids = [account_options.get(name) for name in selected_account_names]
            else:
                st.warning("Nenhuma conta de anúncios encontrada. Verifique se os BMs possuem contas ativas.")

with col3:
    st.markdown("### 3. Filtrar Campanha")
    campaign_filter = st.text_input("Filtrar por nome de Campanha", placeholder="Nome ou termo (ex. CBO-01)")
    
    st.markdown("### 4. Nível dos Dados")
    report_level_name = st.radio(
        "Velocidade vs Detalhe:",
        ["Campanha (Mais rápido, evita quedas)", "Anúncio (Completo, mais lento)"]
    )
    report_level = "campaign" if "Campanha" in report_level_name else "ad"

# Adicionar função de dashboard
def render_dashboard(df, filter_text):
    if df.empty:
        st.warning("Sem dados para exibir o Dashboard.")
        return

    # Clean numeric types
    numeric_cols = ['impressions', 'clicks', 'spend', 'cpm', 'ctr', 'cpc']
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)

    # Filtrar localmente caso haja filtro
    if filter_text:
        # Tenta achar qual coluna é o nome da campanha no dataframe
        search_col = 'campaign_name' if 'campaign_name' in df.columns else None
        
        # Fallback de busca se não tiver 'campaign_name' explícito, procura em qualquer outra coluna txt
        if not search_col and 'ad_name' in df.columns:
             search_col = 'ad_name'
             
        if search_col:
            import re
            # Permite pesquisar mais de um termo separando por vírgulas (ex: "CBO, LAL")
            terms = [re.escape(t.strip()) for t in str(filter_text).replace(';', ',').split(',') if t.strip()]
            if terms:
                pattern = '|'.join(terms)
                st.info(f"Filtro aplicado (Regex Pattern): `{pattern}` na coluna `{search_col}`")
                df[search_col] = df[search_col].astype(str)
                df = df[df[search_col].str.contains(pattern, case=False, na=False, regex=True)]
        else:
             st.warning("Aviso: A API retornou dados sem a coluna de nome da campanha para filtragem.")
             
    if df.empty:
         st.warning("Nenhum dado sobrou após aplicar o filtro de Campanha de CBO.")
         return

    label_col = 'ad_name' if 'ad_name' in df.columns else 'campaign_name'
    if label_col not in df.columns:
        label_col = df.columns[0]
        
    # Agrupar se o df for muito grande e houver a coluna campaign_name
    if len(df) > 30 and 'campaign_name' in df.columns:
        df = df.groupby('campaign_name').sum(numeric_only=True).reset_index()
        label_col = 'campaign_name'
        
    fig = make_subplots(
        rows=2, cols=2, 
        subplot_titles=("Spend Overview", "CPM by Ad", "CTR Performance", "Impressions vs Clicks")
    )

    if 'spend' in df.columns:
        fig.add_trace(go.Bar(x=df[label_col], y=df['spend'], name="Spend"), row=1, col=1)

    if 'cpm' in df.columns:
        fig.add_trace(go.Bar(x=df[label_col], y=df['cpm'], name="CPM", marker_color='orange'), row=1, col=2)

    if 'ctr' in df.columns:
        fig.add_trace(go.Bar(x=df[label_col], y=df['ctr'], name="CTR", marker_color='green'), row=2, col=1)

    if 'impressions' in df.columns and 'clicks' in df.columns:
        fig.add_trace(go.Scatter(x=df['impressions'], y=df['clicks'], mode='markers', text=df[label_col], name="Imp vs Clicks", marker=dict(size=10, color='red')), row=2, col=2)

    fig.update_layout(
        height=700,
        showlegend=False,
        template="plotly_dark"
    )

    st.plotly_chart(fig, use_container_width=True)
    st.dataframe(df, use_container_width=True)

st.markdown("---")

# Botão principal de ação
if st.button("🚀 Importar & Gerar Dashboard", type="primary"):
    if not selected_account_ids:
        st.error("É necessário selecionar ao menos uma Conta de Anúncios para iniciar!")
    else:
        with st.status("Processando dados de API...", expanded=True) as status:
            st.write("Conectando no Facebook Ads API...")
            
            progress_text = st.empty()
            
            def update_progress(info):
                if isinstance(info, int):
                    progress_text.write(f"📥 Baixando páginas... {info} anúncios coletados até agora.")
                else:
                    progress_text.error(str(info))
            
            all_fb_data = []
            for acc_id in selected_account_ids:
                progress_text.write(f"🔍 Coletando dados da conta: {acc_id}")
                padded_date_end = date_end + timedelta(days=1)
                fb_data = fb_api.get_ad_insights(
                    acc_id, 
                    date_start.strftime('%Y-%m-%d'), 
                    padded_date_end.strftime('%Y-%m-%d'),
                    level=report_level,
                    progress_callback=update_progress
                )
                if fb_data:
                    for item in fb_data:
                        item['account_id'] = acc_id
                    all_fb_data.extend(fb_data)
            
            if not all_fb_data:
                 status.update(label="Falha na Importação ou Nenhuma Campanha no Período", state="error", expanded=True)
                 st.error("Nenhum dado retornado. Verifique se o Token expirou ou se de fato não há anúncios ativos neste mês para as contas selecionadas.")
            else:
                 st.write("Carregando Dados...")
                 status.update(label="Dados extraídos com Sucesso!", state="complete", expanded=False)
            
        if all_fb_data:
            st.success(f"Foram encontrados {len(all_fb_data)} anúncios somando {len(selected_account_ids)} conta(s)!")
            
            # Transform data into pandas DF for Dashboard plotting
            df = pd.DataFrame(all_fb_data)
            
            with st.expander("🛠️ Debug: Ver Tabela de Dados Brutos da API (Antes do Filtro)"):
                st.dataframe(df)

            st.markdown("## 📊 Dashboard de Performance")
            render_dashboard(df, campaign_filter)

st.markdown("---")
st.markdown("## 📝 Preencher Planilha (TC, Métricas e Gasto)")
st.markdown("Lê os anúncios na **Coluna B** da aba `032026` e preenche a **Coluna A** apenas se estiver Vazia (mantendo as já mescladas). Também processa a Tabela de Métricas (**Hook Rate, Body View, CPM, CTR, CPC, Gasto**) APENAS para as linhas onde a **Coluna M (Status)** for escrita como `TESTE`.")

if st.button("✍️ Preencher Planilha", type="primary"):
    from fill_creative_tests import fill_creative_tests
    
    if not selected_account_ids:
        st.error("Selecione uma ou mais Contas de Anúncios antes de preencher a planilha.")
    else:
        with st.status("Preenchendo planilha...", expanded=True) as fill_status:
            fill_progress = st.empty()

            def fill_progress_cb(msg):
                fill_progress.write(f"⏳ {msg}")

            try:
                padded_date_end = date_end + timedelta(days=1)
                result = fill_creative_tests(
                    account_ids=selected_account_ids,
                    date_start=date_start.strftime('%Y-%m-%d'),
                    date_end=padded_date_end.strftime('%Y-%m-%d'),
                    fb_token=fb_token,
                    redtrack_token=rt_token,
                    progress_callback=fill_progress_cb,
                )
                fill_status.update(label="Planilha preenchida com sucesso! ✅", state="complete", expanded=False)
                pe_msg = f"\n\n📈 PRÉ-ESCALA: {result.get('filled_pre_escala', 0)} criativos preenchidos (Gasto, Vendas, ROAS, CPA)." if result.get('filled_pre_escala', 0) > 0 else ""
                st.success(f"✅ Sucesso! Foram preenchidos {result['filled_metrics']} Testes Completos (E, F, G, H, I, J).\n\n📊 {result['filled_a']} novas marcações criadas na Coluna A.\n⏭️ {result['skipped_rows']} linhas foram puladas (Coluna M diferente de TESTE).{pe_msg}")
                
                if result['not_found']:
                    with st.expander(f"⚠️ {len(result['not_found'])} Anúncios da Coluna B não encontrados nas campanhas:"):
                        st.write("Verifique se o nome exato inserido na planilha existe no final do nome de alguma campanha ativa desta conta:")
                        st.write("`, `".join(result['not_found']))
                        
            except Exception as e:
                fill_status.update(label="Erro ao preencher planilha", state="error", expanded=True)
                st.error(f"❌ Erro: {e}")
