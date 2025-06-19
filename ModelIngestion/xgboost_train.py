import pandas as pd
from sklearn.model_selection import train_test_split
import xgboost as xgb
from sklearn.metrics import average_precision_score, classification_report
import matplotlib.pyplot as plt

# Load processed data (update the path to your output Parquet file)
df = pd.read_parquet('s3://fraud-data-bucket-pip/output/')

# Feature selection: use all columns except 'Class' as features
feature_cols = [col for col in df.columns if col != 'Class']
X = df[feature_cols]
y = df['Class']

# Train/test split
X_train, X_test, y_train, y_test = train_test_split(
    X, y, stratify=y, test_size=0.2, random_state=42
)

# Train XGBoost model (with class imbalance handling)
model = xgb.XGBClassifier(
    n_estimators=100,
    max_depth=6,
    learning_rate=0.1,
    scale_pos_weight=(len(y_train) - sum(y_train)) / sum(y_train),
    use_label_encoder=False,
    eval_metric='aucpr'
)
model.fit(X_train, y_train)

# Evaluate
y_pred = model.predict(X_test)
y_proba = model.predict_proba(X_test)[:, 1]
print('AUPRC:', average_precision_score(y_test, y_proba))
print(classification_report(y_test, y_pred))

# Feature importance plot
xgb.plot_importance(model, max_num_features=15)
plt.tight_layout()
plt.show()
