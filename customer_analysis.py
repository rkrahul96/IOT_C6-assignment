# # <font color="brown"> Customer Analysis - Spend Prediction and Segmentation </font>

# <b> <font color="blue"> The exercise comprises of 2 major steps.
#  1. Use the mall_customers.csv file to build a model for predicting the spend given gender, age and salary
#  2. Build a model to cluster the customers into multiple groups and predict the customer segment for a new customer.</font>

# <b> Importing all required packages

import pandas as pd
import numpy as np

import matplotlib.pyplot as plt
from mpl_toolkits import mplot3d

from sklearn.cluster import KMeans
from sklearn import metrics
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import silhouette_score
from sklearn.metrics import confusion_matrix
from imblearn.over_sampling import RandomOverSampler

INFILE="./customer_data.csv"
# <b> create a dataframe by reading the csv and check for bad records
def create_cust_df(INFILE):
    cust_df = pd.read_csv(INFILE,index_col=0)
    if ( cust_df['Gender'].isna().sum() == 0 & cust_df['Age'].isna().sum() == 0 & 
        cust_df['Annual Income (k$)'].isna().sum() == 0 & cust_df['Spending Score (1-100)'].isna().sum() == 0):
        print ("No NA data present.")
        if (cust_df['Gender'].isnull().sum() == 0 & cust_df['Age'].isnull().sum() == 0 & 
            cust_df['Annual Income (k$)'].isnull().sum() == 0 & cust_df['Spending Score (1-100)'].isnull().sum() == 0):
            print("No null data present")
        else:
            print("need to clean for nulls")
    else:
        print("need to clean for NA")
    return cust_df


# <b> Data is clean. No need to do any cleaning. Yaay!!
# ### <b> We need to do clustering. Let's try k-means first
# ### Create a new DF to use all the 4 variables to create cluster - gender, age, salary, spend
def create_cluster(cust_df):
    cstdf=cust_df
    cstdf=cstdf.replace('Male',0)
    cstdf=cstdf.replace('Female',1)
    #cstdf['Gender'].value_counts()
    # <b> performing k-means clustering using 6 clusters.
    k_means = KMeans(n_clusters=6)
    k_means.fit(cstdf)
    # #### Unique cluster values are listed below. Use this to add to the original dataframe later
    #labels=np.unique(k_means.labels_)
    #print("unique cluster labels : " , labels)
    cluster_labels=k_means.labels_
    #print("cluster labels for mall customer data : ", cluster_labels)
    # <b> find cluster score
    #score = silhouette_score (cstdf, k_means.labels_)
    #print("Score = ", score)
    # <b> adding the cluster labels back to the original dataframe
    cstdf['cluster']=cluster_labels
    #print(cstdf.columns)
    #print(cstdf.head())
    return cstdf, k_means

# <b> Plot the custer in 3D to check for maps. The same can also be done in a 2D plot.

def plot_cluster(k_means, cstdf):
    centers = k_means.cluster_centers_
    #print("cluster centers : ", centers)

    fig = plt.figure(figsize=(12, 8))
    ax = plt.axes(projection ="3d") 
    ax.scatter3D(cstdf['Age'],cstdf['Annual Income (k$)'],cstdf['Spending Score (1-100)'],c=k_means.labels_, s=50)
    ax.scatter3D(centers[:,1],centers[:,2], centers[:,3], color='purple', marker='s', s=150) 
    ax.set_xlabel('Age')
    ax.set_ylabel('Annual Income (k$)')
    ax.set_zlabel('Spending Score (1-100)')
    plt.title('K-means Age vs Annual Income (k$) vs Spending Score (1-100)')
    plt.show()

# ### Counts for our different clusters
#cstdf['cluster'].value_counts()
# # Approach for classification using Random Forest
# <b> Convert the categorical variable first to a numeric. here only 2 possibilities
def generate_spend(cust_df):
    cust_df=cust_df.replace('Male',0)
    cust_df=cust_df.replace('Female',1)
    cust_df.head()
    # ### Get the features and Labels first using the original dataframe. 
    # ### 1. Features are the input parameters - In our case, gender, age and income
    # ### 2. Labels are the output parameter - Prediction parameter - In our case Spending
    features=cust_df[['Gender', 'Age', 'Annual Income (k$)']] #Features
    labels=cust_df[['Spending Score (1-100)']] #Labels
    # ### Find distribution on the spending score 
    #freq = dict(cust_df['Spending Score (1-100)'].value_counts().sort_values(ascending=False))
    #plt.bar(list(freq.keys()), list(freq.values()))
    #target = cust_df['Spending Score (1-100)'].unique()
    #print(target)
    # ### Apply oversampling to simulate low occurrence samples and plot it to check. We are doing this since most of the spending scores appears only once. This makes the data imbalanced. Oversampling will generate a new point close to the original points in space. Use this to fit in the distribution and check.
    ros = RandomOverSampler()
    feature_samples, label_samples = ros.fit_sample(features, labels)
    #print("Feature count : " , feature_samples.count())
    #print("Label count : ", label_samples.count())
    freq = dict(label_samples['Spending Score (1-100)'].value_counts().sort_values(ascending=False))
    plt.bar(list(freq.keys()), list(freq.values()))
    # <b> split data into training and test data based on 90:10 ratio
    x_train, x_test, y_train, y_test = train_test_split(feature_samples, np.ravel(label_samples), test_size=0.1)
    # <b> Create a Gaussian Classifier with number of estimators as 100
    clf=RandomForestClassifier(n_estimators=100)
    # <b> Train the model using the training sets. Generate predictions on the test set. The data should be array and not dataframe
    clf.fit(x_train.values,y_train)
    # ## <font color="purple"> How to predict scoring using the classifier for new records. Supply the incoming records as an ndarray. Apply the predict function. Example below on the test data </font>
    return clf

# <b> check the accuracy using actual and predicted values. y_pred is the predicted values and y_test is the sample actual values
def get_accuracy(y_pred,y_test):
    #print("Accuracy:",metrics.accuracy_score(y_pred, y_test))
    return metrics.accuracy_score(y_pred, y_test)

#calculates true positives. Ideal state the diagonals should have non-zero values while the rest should be 0
#confusion_matrix(y_test, y_pred) 
# # For any new incoming data, apply the y_pred=clf.predict(x_test.values) to predict the spending score

if __name__ == "__main__":
    cust_df=create_cust_df(INFILE)
   
    clf=generate_spend(cust_df)
#spend=clf.predict(x_test.values)
    cust_info=pd.DataFrame([[  0,  34,  78], [  0,  65,  63], [  1,  35,  19]])
    spend=clf.predict(cust_info.values)
    print(spend)

    cstdf,k_means = create_cluster(cust_df)
    clsinput=pd.DataFrame([[0,64,19,3],[1,20,16,6]])
    pred_cluster=k_means.predict(clsinput)
    print(pred_cluster)

    plot_cluster(k_means, cstdf)

# ### Verifying and validating the model is correct. To do this get the values of the different clusters. Apply the model and verify.
#print(cstdf.loc[cstdf['cluster']==0].head())
#print(cstdf.loc[cstdf['cluster']==1].head())

# ## <font color="purple"> How to predict cluster for new records. Supply the incoming records as dataframe directly or as an ndarray. Apply the predict function. Example below on the existing data </font>
    
#print(k_means.predict(pd.DataFrame([[1,20,16,6]])))
#print(k_means.predict(pd.DataFrame([[0,48,39,36]])))
#print(k_means.predict(pd.DataFrame([[1,49,42,52]])))
#print(k_means.predict(pd.DataFrame([[0,47,71,9]])))
