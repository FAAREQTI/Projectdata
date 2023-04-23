# importing EDA packages
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np

# reading data
file_path = "/Users/fatima-ezzahraareqti/Desktop/datascience/Projectdata/data/supermarket_sales.csv"
sales_df = pd.read_csv(file_path)

# defining function to do bivariate analysis


def bivariate_analysis(df, x_col, y_col):
    """ bivariate analysis with a bar plot

    Args:
        df (_type_): dataframe
        x_col (_type_): column name - categorical
        y_col (_type_): column name - numerical
    """
    sns.set_style('whitegrid')
    fig, ax = plt.subplots(figsize=(12, 8))
    sns.barplot(x=x_col, y=y_col, data=df, estimator=np.mean)
    plt.title(f'{x_col} vs {y_col}')
    plt.show()


# Bivariate analysis for: product line VS total
bivariate_analysis(sales_df, 'product_line', 'total')

# Bivariate analysis for: city VS total
bivariate_analysis(sales_df, 'city', 'total')

# Bivariate analysis for: Customer type VS total
bivariate_analysis(sales_df, 'customer_type', 'total')

# Bivariate analysis for: Branch VS rating
bivariate_analysis(sales_df, 'branch', 'rating')

# defining function to do multivariate analysis


def multivariate_analysis(df, x_col, y_col, hue_col):
    """multivariate analysis with box plot

    Args:
        df (_type_): dataframe
        x_col (_type_): column name - categorical
        y_col (_type_): column name - numerical 
        hue_col (_type_): column name - categorical
    """
    sns.set_style('whitegrid')
    fig, ax = plt.subplots(figsize=(12, 8))
    sns.boxplot(x=x_col, y=y_col, hue=hue_col, data=df, ax=ax)
    plt.title(f'{x_col} vs {y_col} by {hue_col}')
    plt.show()


# Multivariate analysis for: product line by city and total
multivariate_analysis(sales_df, "product_line", "total", "city")

# Multivariate analysis for: product line by city and total
multivariate_analysis(sales_df, "product_line", "total", "city")

# Multivariate analysis for: product line by city and total
multivariate_analysis(sales_df, "product_line", "total", "gender")
