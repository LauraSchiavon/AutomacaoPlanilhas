import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import os

def create_dashboard():
    excel_path = r'C:\Preencher planilha\FB - LOTTO V7.xlsx'
    
    # Read the data from "Dados Brutos"
    try:
        df = pd.read_excel(excel_path, sheet_name='Dados Brutos')
    except Exception as e:
        print(f"Error reading Excel file: {e}")
        return

    if df.empty:
        print("No data found to generate dashboard.")
        return

    # Clean the data types
    numeric_cols = ['impressions', 'clicks', 'spend', 'cpm', 'ctr', 'cpc']
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)

    # Make sure we have a label column (e.g. ad_name or campaign_name)
    label_col = 'ad_name' if 'ad_name' in df.columns else 'campaign_name'
    if label_col not in df.columns:
        label_col = df.columns[0] # fallback

    # Dashboard layout
    fig = make_subplots(
        rows=2, cols=2, 
        subplot_titles=("Spend Overview", "CPM by Ad", "CTR Performance", "Impressions vs Clicks")
    )

    # 1. Total Spend (Bar chart)
    if 'spend' in df.columns:
        fig.add_trace(go.Bar(x=df[label_col], y=df['spend'], name="Spend"), row=1, col=1)

    # 2. CPM (Bar chart)
    if 'cpm' in df.columns:
        fig.add_trace(go.Bar(x=df[label_col], y=df['cpm'], name="CPM", marker_color='orange'), row=1, col=2)

    # 3. CTR (Bar chart)
    if 'ctr' in df.columns:
        fig.add_trace(go.Bar(x=df[label_col], y=df['ctr'], name="CTR", marker_color='green'), row=2, col=1)

    # 4. Impressions vs Clicks (Scatter)
    if 'impressions' in df.columns and 'clicks' in df.columns:
        fig.add_trace(go.Scatter(x=df['impressions'], y=df['clicks'], mode='markers', text=df[label_col], name="Imp vs Clicks", marker=dict(size=10, color='red')), row=2, col=2)

    fig.update_layout(
        title_text="Facebook Ads Teste de Criativos - Dashboard",
        height=800,
        showlegend=False,
        template="plotly_dark"
    )

    # Save to HTML
    output_path = r'C:\Preencher planilha\dashboard.html'
    fig.write_html(output_path)
    print(f"Dashboard successfully generated at: {output_path}")

if __name__ == "__main__":
    create_dashboard()
