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
            'fecha': e['fecha'],
            'moneda': e['moneda'],
            'cantidad': float(e['cantidad']),
            'valor_unitario': float(e['valor_unitario'])
        })

    for _, venta in ventas_df.sort_values('fecha').iterrows():
        cant_pendiente = float(venta['cantidad'])
        total_ingreso_venta = float(venta['total eur (tras pagar comisión)'])
        moneda = venta['moneda']
        fecha = venta['fecha']
        
        # --- LÓGICA DE TIPO DE CONTRAPRESTACIÓN (Según imagen Renta) ---
        # Buscamos si en el mismo momento hubo una compra de otra moneda virtual
        vinculadas = df_full[df_full['fecha'] == fecha]
        es_permuta = vinculadas[(vinculadas['tipo'] == 'compra') & (vinculadas['moneda'] != 'EUR')].any().any()
        
        # F: Moneda curso legal | N: Otra moneda virtual
        tipo_contraprestacion = 'N' if es_permuta else 'F'
        
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

# Ejecutar
fifo_detalle = calcular_fifo_estricto(ventas_ganancia, entradas, df)

# --- EXPORTACIÓN ---
with pd.ExcelWriter(archivo_salida, engine='openpyxl') as writer:
    # Resumen Anual
    res_anual = pd.DataFrame({'Año': sorted(df['año'].unique())}).set_index('Año')
    res_anual['Ganancia_Patrimonial'] = fifo_detalle.groupby('Año')['Beneficio/Pérdida'].sum()
    res_anual['Rendimientos_Recompensas'] = df[(df['tipo']=='recompensa') & ~df['es_referral']].groupby('año')['total eur (tras pagar comisión)'].sum()
    res_anual['Rendimientos_Minería'] = df[df['tipo']=='minería'].groupby('año')['total eur (tras pagar comisión)'].sum()
    res_anual['Referral_Commission'] = df[df['es_referral']].groupby('año')['total eur (tras pagar comisión)'].sum()
    res_anual.fillna(0).round(2).reset_index().to_excel(writer, sheet_name="Resumen Anual", index=False)

    # Agrupado por Año (Listo para volcar a la Renta)
    agrupado = fifo_detalle.groupby(['Año', 'Moneda', 'Tipo Contraprestación']).agg({
        'Cantidad Vendida': 'sum',
        'Valor Transmisión': 'sum',
        'Valor Adquisición': 'sum',
        'Beneficio/Pérdida': 'sum'
    }).round(2).reset_index()
    agrupado.to_excel(writer, sheet_name="Agrupado por Año", index=False)

    # Detalle y Ayuda
    fifo_detalle.to_excel(writer, sheet_name="FIFO Tracking Detallado", index=False)
    pd.DataFrame([
        {'Letra': 'F', 'Descripción': 'Moneda de curso legal (Euros)', 'Uso': 'Venta directa a €'},
        {'Letra': 'N', 'Descripción': 'Otra moneda virtual', 'Uso': 'Permuta (cambio por otra cripto)'},
        {'Letra': 'O', 'Descripción': 'Otro activo virtual', 'Uso': 'NFTs u otros activos'},
        {'Letra': 'B', 'Descripción': 'Bienes o servicios', 'Uso': 'Pago de compras con cripto'}
    ]).to_excel(writer, sheet_name="Leyenda Tipos Renta", index=False)

print(f"✅ Archivo '{archivo_salida}' generado. Usa la columna 'Tipo Contraprestación' para el desplegable de la Renta.")
