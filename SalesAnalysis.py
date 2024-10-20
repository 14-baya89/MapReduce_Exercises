from mrjob.job import MRJob
from mrjob.step import MRStep
import csv

class SalesAnalysis(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper_transactions,
                   reducer=self.reducer_count_transactions),
            MRStep(mapper=self.mapper_sales_per_product,
                   reducer=self.reducer_sales_per_product),
            MRStep(reducer=self.reducer_find_max_sales)
        ]

    # Étape 1 : Compter les transactions
    def mapper_transactions(self, _, line):
        # Utilisation du module csv pour lire la ligne en tant que liste
        reader = csv.reader([line])
        for row in reader:
            if row[0] != 'TransactionID':  # Ignorer la ligne d'en-tête
                yield "total_transactions", 1

    def reducer_count_transactions(self, key, values):
        # Calculer le nombre total de transactions
        yield key, sum(values)

    # Étape 2 : Calculer les ventes totales par produit
    def mapper_sales_per_product(self, _, line):
        reader = csv.reader([line])
        for row in reader:
            if row[0] != 'TransactionID':
                product = row[2]
                quantity = int(row[3])
                unit_price = float(row[4])
                total_sale = quantity * unit_price
                yield product, total_sale

    def reducer_sales_per_product(self, product, sales):
        # Agréger les ventes pour chaque produit
        yield None, (sum(sales), product)

    # Étape 3 : Trouver le produit avec les ventes les plus élevées
    def reducer_find_max_sales(self, _, product_sales_pairs):
        # Trouver le produit ayant généré le chiffre d'affaires le plus élevé
        yield max(product_sales_pairs)

if __name__ == '__main__':
    SalesAnalysis.run()
