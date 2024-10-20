from mrjob.job import MRJob
from mrjob.step import MRStep
import csv

class ProductReviewAnalysis(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper_ratings,
                   reducer=self.reducer_average_ratings),
            MRStep(reducer=self.reducer_best_and_worst_products)
        ]

    # Étape 1 : Mapper les notes pour chaque produit
    def mapper_ratings(self, _, line):
        # Utilisation du module csv pour lire la ligne
        reader = csv.reader([line])
        for row in reader:
            if row[0] != 'ReviewID':  # Ignorer la ligne d'en-tête
                product = row[1]
                rating = float(row[3])  # Convertir la note en float
                yield product, rating

    # Étape 2 : Calculer la moyenne des notes par produit
    def reducer_average_ratings(self, product, ratings):
        ratings_list = list(ratings)
        avg_rating = sum(ratings_list) / len(ratings_list)
        yield None, (avg_rating, product)

    # Étape 3 : Trouver les produits avec les meilleures et pires notes
    def reducer_best_and_worst_products(self, _, product_rating_pairs):
        sorted_products = sorted(product_rating_pairs, reverse=True)
        best_product = sorted_products[0]
        worst_product = sorted_products[-1]
        
        yield "Best Product", best_product
        yield "Worst Product", worst_product

if __name__ == '__main__':
    ProductReviewAnalysis.run()