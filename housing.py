"""Problem Statement Real estate agents want help to predict the house price for regions in the USA.
He gave you the dataset to work on and you decided to use the Linear Regression Model. Create a
model that will help him to estimate what the house would sell for."""

import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_squared_error, r2_score

# Step 1: Load the dataset
df = pd.read_csv("USA_Housing.csv")  # Make sure this file is in the same folder

# Step 2: Select features (drop 'Price' and 'Address')
X = df.drop(['Price', 'Address'], axis=1)
y = df['Price']

# Step 3: Split into training and test sets
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

# Step 4: Train the Linear Regression model
model = LinearRegression()
model.fit(X_train, y_train)

# Step 5: Make predictions
predictions = model.predict(X_test)

# Step 6: Evaluate the model
print("Mean Squared Error:", mean_squared_error(y_test, predictions))
print("R-squared Score:", r2_score(y_test, predictions))

# Optional: Predict for a new house
new_house = [[60000, 5, 7, 4, 30000]]  # Example input
predicted_price = model.predict(new_house)
print("Predicted house price:", predicted_price[0])
