import pandas as pd

archivo_entrada = "Cripto_Control_Fiscal.xlsx"
archivo_salida = "resumen_fiscal_crypto_ESPANA.xlsx"

# Cargar datos
df = pd.read_excel(archivo_entrada, sheet_name="Transacciones")

# --- NORMALIZACIÓN ---
df.columns = df.columns.str.strip().str.lower()
df['tipo'] = df['tipo'].str.strip().str.lower()
df['moneda'] = df['moneda'].str.strip().str.upper()
df['fecha'] = pd.to_datetime(df['fecha'], dayfirst=True, errors='coerce')
df['comentario'] = df['comentario'].fillna('').str.strip()
df['año'] = df['fecha'].dt.year

# Identificar Referidos (tributan en Base General)
df['es_referral'] = df['tipo'].str.contains('referral', case=False, na=False)

# --- PREPARACIÓN FIFO ---
entradas = df[df['tipo'].isin(['compra', 'recompensa', 'minería']) & ~df['es_referral']].copy()
entradas['valor_unitario'] = entradas['total eur (tras pagar comisión)'].fillna(0) / entradas['cantidad'].replace(0, 1)

ventas_ganancia = df[(df['tipo'] == 'venta') & (df['moneda'] != 'EUR') & ~df['es_referral']].copy()

def calcular_fifo_estricto(ventas_df, entradas_df, df_full):
    inventario = []
    detalle = []

    for _, e in entradas_df.sort_values('fecha').iterrows():
        inventario.append({
            'fecha': e['fecha'], 'moneda': e['moneda'],
            'cantidad': float(e['cantidad']), 'valor_unitario': float(e['valor_unitario'])
        })

    for _, venta in ventas_df.sort_values('fecha').iterrows():
        cant_pendiente = float(venta['cantidad'])
        total_ingreso_venta = float(venta['total eur (tras pagar comisión)'])
        moneda = venta['moneda']
        fecha = venta['fecha']
        comentario = venta['comentario'].lower()
        
        # --- LÓGICA DE CONTRAPRESTACIÓN (+/- 5s) ---
        margen_inicio = fecha - pd.Timedelta(seconds=5)
        margen_fin = fecha + pd.Timedelta(seconds=5)
        vinculadas = df_full[(df_full['fecha'] >= margen_inicio) & (df_full['fecha'] <= margen_fin)]
        
        tiene_compra_cripto = vinculadas[(vinculadas['tipo'] == 'compra') & (vinculadas['moneda'] != 'EUR')].any().any()
        es_permuta_por_texto = "exchange" in comentario and "for" in comentario and "eur" not in comentario
        
        # N: Otra moneda virtual | F: Moneda de curso legal (Euros)
        tipo_contraprestacion = 'N' if (tiene_compra_cripto or es_permuta_por_texto) else 'F'
        
        precio_venta_unitario = total_ingreso_venta / cant_pendiente if cant_pendiente > 0 else 0

        i = 0
        while cant_pendiente > 1e-9 and i < len(inventario):
            item = inventario[i]
            if item['moneda'] == moneda and item['fecha'] <= fecha:
                usado = min(cant_pendiente, item['cantidad'])
                coste_adq = usado * item['valor_unitario']
                val_trans = usado * precio_venta_unitario

                detalle.append({
                    'Año': fecha.year,
                    'Fecha Venta': fecha,
                    'Moneda': moneda,
                    'Cantidad Vendida': usado,
                    'Valor Transmisión': round(val_trans, 4),
                    'Valor Adquisición': round(coste_adq, 4),
                    'Beneficio/Pérdida': round(val_trans - coste_adq, 4),
                    'Tipo Contraprestación': tipo_contraprestacion
                })
                item['cantidad'] -= usado
                cant_pendiente -= usado
                if item['cantidad'] <= 1e-9:
                    inventario.pop(i)
                    continue 
            i += 1
    return pd.DataFrame(detalle)

# Ejecutar cálculo
fifo_detalle = calcular_fifo_estricto(ventas_ganancia, entradas, df)

# --- EXPORTACIÓN ---
with pd.ExcelWriter(archivo_salida, engine='openpyxl') as writer:
    # 1. Resumen Anual
    res_anual = pd.DataFrame({'Año': sorted(df['año'].unique())}).set_index('Año')
    res_anual['Ganancia_Patrimonial'] = fifo_detalle.groupby('Año')['Beneficio/Pérdida'].sum()
    res_anual['Rendimientos_Recompensas'] = df[(df['tipo']=='recompensa') & ~df['es_referral']].groupby('año')['total eur (tras pagar comisión)'].sum()
    res_anual['Rendimientos_Minería'] = df[df['tipo']=='minería'].groupby('año')['total eur (tras pagar comisión)'].sum()
    res_anual['Referral_Commission'] = df[df['es_referral']].groupby('año')['total eur (tras pagar comisión)'].sum()
    res_anual.fillna(0).round(2).reset_index().to_excel(writer, sheet_name="Resumen Anual", index=False)

    # 2. Agrupado por Año (Casillas 1800+)
    agrupado = fifo_detalle.groupby(['Año', 'Moneda', 'Tipo Contraprestación']).agg({
        'Cantidad Vendida': 'sum',
        'Valor Transmisión': 'sum',
        'Valor Adquisición': 'sum',
        'Beneficio/Pérdida': 'sum'
    }).round(2).reset_index()
    agrupado.to_excel(writer, sheet_name="Agrupado por Año", index=False)

    # 3. FIFO Detallado
    fifo_detalle.to_excel(writer, sheet_name="FIFO Tracking Detallado", index=False)
    
    # 4. INSTRUCCIONES RENTA ESPAÑA
    instrucciones_data = [
        {
            'Concepto': "Ganancia/Pérdida Patrimonial (Ventas y Permutas)",
            'Casillas Renta': "1800 a 1814",
            'Origen en Excel': "Pestaña 'Agrupado por Año'",
            'Nota Fiscal': "Usa 'F' para ventas a Euros y 'N' para cambios entre criptos."
        },
        {
            'Concepto': "Rendimientos de Recompensas (Staking / Earn)",
            'Casillas Renta': "0033",
            'Origen en Excel': "Pestaña 'Resumen Anual' -> Rendimientos_Recompensas",
            'Nota Fiscal': "Tributan como Rendimientos del Capital Mobiliario (Base del Ahorro)."
        },
        {
            'Concepto': "Rendimientos de Minería (No profesional)",
            'Casillas Renta': "0033",
            'Origen en Excel': "Pestaña 'Resumen Anual' -> Rendimientos_Minería",
            'Nota Fiscal': "Valor en EUR al momento de recibir la moneda."
        },
        {
            'Concepto': "Comisiones de Referidos (Referral)",
            'Casillas Renta': "0304",
            'Origen en Excel': "Pestaña 'Resumen Anual' -> Referral_Commission",
            'Nota Fiscal': "Tributan en la Base General (Otros ingresos)."
        }
    ]
    pd.DataFrame(instrucciones_data).to_excel(writer, sheet_name="Instrucciones Renta España", index=False)

print(f"✅ Archivo '{archivo_salida}' generado con éxito.")
