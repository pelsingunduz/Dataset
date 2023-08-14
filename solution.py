import pandas as pd
import argparse

def main():
    # Komut satırı argümanlarını işleme alma
    parser = argparse.ArgumentParser(description='Process sales data within a date range.')
    parser.add_argument('--min-date', type=str, default='2021-01-08', help='Start of the date range. Format: "YYYY-MM-DD".')
    parser.add_argument('--max-date', type=str, default='2021-05-30', help='End of the date range. Format: "YYYY-MM-DD".')
    args = parser.parse_args()

    # Tarih aralığını pandas datetime formatına dönüştürme
    min_date = pd.to_datetime(args.min_date)
    max_date = pd.to_datetime(args.max_date)

    # Satış verilerini okuma
    df = pd.read_csv('sales.csv')
    df['date'] = pd.to_datetime(df['date'])

    # Ürün bazında hareketli ortalama (MA7_P) ve gecikmeli değer (LAG7_P) hesaplama
    df['MA7_P'] = df.groupby('product')['quantity'].transform(lambda x: x.rolling(window=7).mean())
    df['LAG7_P'] = df.groupby('product')['quantity'].transform(lambda x: x.shift(7))

    # Marka verilerini okuma
    brands_df = pd.read_csv('brand.csv')
    
    # Satış verileri ile marka verilerini "product" sütunu üzerinden birleştirme
    merged_brand = df.merge(brands_df, left_on='product', right_on='id')
    
    # Mağaza, marka ve tarih bazında toplam satış miktarlarını hesaplama
    brand_agg = merged_brand.groupby(['store', 'id', 'date'])['quantity'].sum().reset_index()
    
    # Mağaza ve marka bazında hareketli ortalama (MA7_B) ve gecikmeli değer (LAG7_B) hesaplama
    brand_agg['MA7_B'] = brand_agg.groupby(['store', 'id'])['quantity'].transform(lambda x: x.rolling(window=7).mean())
    brand_agg['LAG7_B'] = brand_agg.groupby(['store', 'id'])['quantity'].transform(lambda x: x.shift(7))
    brand_agg['date'] = pd.to_datetime(brand_agg['date'])

    # Mağaza verilerini okuma
    stores_df = pd.read_csv('store.csv')
    
    # Satış verileri ile mağaza verilerini "store" sütunu üzerinden birleştirme
    merged_store = df.merge(stores_df, left_on='store', right_on='id')
    
    # Mağaza bazında toplam satış miktarlarını hesaplama
    store_agg = merged_store.groupby(['store', 'date'])['quantity'].sum().reset_index()
    
    # Mağaza bazında hareketli ortalama (MA7_S) ve gecikmeli değer (LAG7_S) hesaplama
    store_agg['MA7_S'] = store_agg.groupby('store')['quantity'].transform(lambda x: x.rolling(window=7).mean())
    store_agg['LAG7_S'] = store_agg.groupby('store')['quantity'].transform(lambda x: x.shift(7))
    store_agg['date'] = pd.to_datetime(store_agg['date'])

    # Sonuç veri çerçevelerini birleştirme
    result = df.merge(brand_agg, on=['store', 'date'], how='left', suffixes=('', '_brand'))
    result = result.merge(store_agg, on=['store', 'date'], how='left', suffixes=('', '_store'))

    # Sonuç veri çerçevesini seçme ve sütun adlarını düzenleme
    result = result[['product', 'store', 'id', 'date', 'quantity', 'MA7_P', 'LAG7_P', 'quantity_brand', 'MA7_B', 'LAG7_B', 'quantity_store', 'MA7_S', 'LAG7_S']]
    result.columns = ['product_id', 'store_id', 'brand_id', 'date', 'sales_product', 'MA7_P', 'LAG7_P', 'sales_brand', 'MA7_B', 'LAG7_B', 'sales_store', 'MA7_S', 'LAG7_S']

    # Sonuçları CSV dosyasına yazma
    result.to_csv('features.csv', index=False)

if __name__ == "__main__":
    main()