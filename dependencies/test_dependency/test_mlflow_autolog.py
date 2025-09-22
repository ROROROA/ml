import mlflow
from sklearn.model_selection import train_test_split
from sklearn.datasets import load_diabetes
from sklearn.ensemble import RandomForestRegressor

# 设置 MLflow Tracking Server 的 URI
mlflow.set_tracking_uri("http://localhost:5001")

# set the experiment id
# Note: It's generally better to use experiment names
# but we will stick to the user's code for now.
try:
    mlflow.set_experiment(experiment_id="2")
except mlflow.exceptions.MlflowException:
    print("Experiment with ID '2' might not exist, creating a new experiment.")
    mlflow.set_experiment("Default Experiment for Autolog")


mlflow.autolog()

print("Loading dataset...")
db = load_diabetes()
X_train, X_test, y_train, y_test = train_test_split(db.data, db.target)
print("Dataset loaded and split.")

# Create and train models.
print("Training RandomForestRegressor model...")
rf = RandomForestRegressor(n_estimators=100, max_depth=6, max_features=3)
rf.fit(X_train, y_train)
print("Model training finished.")

# Use the model to make predictions on the test dataset.
print("Making predictions...")
predictions = rf.predict(X_test)
print("Predictions made.")

print("\nScript finished. Check the MLflow UI to see the autologged run.")
