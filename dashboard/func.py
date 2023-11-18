class DataAnalyzer:
    def __init__(self, df):
        self.df = df

    def create_daily_orders_df(self):
        # Resampling dan agregasi data
        daily_orders_df = (
            self.df.resample(rule="D", on="order_approved_at")
            .agg(order_count=("order_id", "nunique"), revenue=("payment_value", "sum"))
            .reset_index()
        )

        # Mengubah nama kolom
        daily_orders_df.rename(
            columns={"order_count": "order_count", "revenue": "revenue"}, inplace=True
        )

        return daily_orders_df

    def create_sum_spend_df(self):
        daily_total_spend = (
            self.df.resample(rule="D", on="order_approved_at")
            .agg(total_spend=("payment_value", "sum"))
            .reset_index()
        )
        return daily_total_spend

    def create_sum_order_items_df(self):
        sales_products = (
            self.df[self.df["order_status"] == "delivered"]
            .groupby("product_category_name_english")["product_id"]
            .count()
            .reset_index()
            .rename(columns={"product_id": "jumlah_terjual"})
            .sort_values(by="jumlah_terjual", ascending=False)
        )

        return sales_products

    def review_score_df(self):
        review_scores = (
            self.df["review_score"].value_counts().sort_values(ascending=False)
        )
        most_common_score = review_scores.idxmax()

        return review_scores, most_common_score

    def create_bystate_df(self):
        # Menghitung jumlah customer unik per state
        customer_count_by_state = (
            self.df.groupby(by="customer_state")["customer_id"].nunique().reset_index()
        )
        customer_count_by_state.rename(
            columns={"customer_id": "customer_count"}, inplace=True
        )

        # Mendapatkan state dengan jumlah customer terbanyak
        most_common_state = customer_count_by_state.loc[
            customer_count_by_state["customer_count"].idxmax(), "customer_state"
        ]

        # Mengurutkan DataFrame berdasarkan customer_count
        customer_count_by_state = customer_count_by_state.sort_values(
            by="customer_count", ascending=False
        )

        return customer_count_by_state, most_common_state

    def create_order_status(self):
        # Menghitung jumlah status pesanan
        order_status_count = (
            self.df["order_status"].value_counts().sort_values(ascending=False)
        )

        # Mendapatkan status pesanan yang paling umum
        most_common_status = order_status_count.idxmax()

        return order_status_count, most_common_status


class BrazilMapPlotter:
    def __init__(self, data, plt, mpimg, urllib, st):
        self.data = data
        self.plt = plt
        self.mpimg = mpimg
        self.urllib = urllib
        self.st = st

    def plot(self):
        brazil = self.mpimg.imread(
            self.urllib.request.urlopen(
                "https://i.pinimg.com/originals/3a/0c/e1/3a0ce18b3c842748c255bc0aa445ad41.jpg"
            ),
            "jpg",
        )
        ax = self.data.plot(
            kind="scatter",
            x="geolocation_lng",
            y="geolocation_lat",
            figsize=(10, 10),
            alpha=0.3,
            s=0.3,
            c="red",
        )
        self.plt.axis("off")
        self.plt.imshow(brazil, extent=[-73.98283055, -33.8, -33.75116944, 5.4])
        self.st.pyplot()
