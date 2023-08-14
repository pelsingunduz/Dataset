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

    # Belirli bir tarih aralığına göre satış verilerini filtreleme
    filtered_df = df[(df['date'] >= min_date) & (df['date'] <= max_date)]

    #Marka ve mağaza verilerini okuma
    brands_df = pd.read_csv('brand.csv')
    stores_df = pd.read_csv('store.csv')

    # Satış verileri ile marka verilerini "product" sütunu üzerinden birleştirme
    merged_brand = filtered_df.merge(brands_df, left_on='product', right_on='id')

    # Mağaza verileri ile mağaza verilerini "store" sütunu üzerinden birleştirme
    merged_store = filtered_df.merge(stores_df, left_on='store', right_on='id')

    # Mağaza, marka ve tarih bazında toplam satış miktarlarını hesaplama 
    brand_agg = merged_brand.groupby(['store', 'id', 'date'])['quantity'].sum().reset_index()
    brand_agg = brand_agg[(brand_agg['date'] >= min_date) & (brand_agg['date'] <= max_date)]

    # Mağaza ve marka bazında hareketli ortalama (MA7_B) ve gecikmeli değer (LAG7_B) hesaplama 
    brand_agg['MA7_B'] = brand_agg.groupby(['store', 'id'])['quantity'].transform(lambda x: x.rolling(window=7, min_periods=1).mean())
    brand_agg['LAG7_B'] = brand_agg.groupby(['store', 'id'])['quantity'].transform(lambda x: x.shift(7))
    brand_agg['date'] = pd.to_datetime(brand_agg['date'])

    # Mağaza bazında toplam satış miktarlarını hesaplama 
    store_agg = merged_store.groupby(['store', 'date'])['quantity'].sum().reset_index()
    store_agg = store_agg[(store_agg['date'] >= min_date) & (store_agg['date'] <= max_date)]

    # Mağaza bazında hareketli ortalama (MA7_S) ve gecikmeli değer (LAG7_S) hesaplama 
    store_agg['MA7_S'] = store_agg.groupby('store')['quantity'].transform(lambda x: x.rolling(window=7, min_periods=1).mean())
    store_agg['LAG7_S'] = store_agg.groupby('store')['quantity'].transform(lambda x: x.shift(7))
    store_agg['date'] = pd.to_datetime(store_agg['date'])

    # Sonuç veri çerçevelerini birleştirme
    result = filtered_df.merge(brand_agg, on=['store', 'date'], how='left', suffixes=('', '_brand'))
    result = result.merge(store_agg, on=['store', 'date'], how='left', suffixes=('', '_store'))

    # Sonuçları CSV dosyasına yazma
    result.to_csv('features.csv', index=False)

if __name__ == "__main__":
    main()
