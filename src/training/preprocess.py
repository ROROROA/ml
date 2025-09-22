# src/training/preprocess.py
from typing import Dict, List
import pandas as pd

from ray.data.preprocessor import Preprocessor
from sklearn.preprocessing import StandardScaler, OneHotEncoder
from sklearn.compose import ColumnTransformer

class CustomPreprocessor(Preprocessor):
    """
    一个自定义的 Ray Preprocessor 示例。
    它封装了 scikit-learn 的 ColumnTransformer，用于处理混合数据类型。
    """

    def __init__(self, numeric_columns: List[str], categorical_columns: List[str]):
        self.numeric_columns = numeric_columns
        self.categorical_columns = categorical_columns
        self.transformer = None

    def _fit(self, dataset: pd.DataFrame) -> Preprocessor:
        # 定义如何处理数值和类别特征
        numeric_transformer = StandardScaler()
        categorical_transformer = OneHotEncoder(handle_unknown='ignore')

        # 使用 ColumnTransformer 来组合这些步骤
        self.transformer = ColumnTransformer(
            transformers=[
                ('num', numeric_transformer, self.numeric_columns),
                ('cat', categorical_transformer, self.categorical_columns)
            ],
            remainder='passthrough' # 保留其他列
        )
        
        self.transformer.fit(dataset)
        return self

    def _transform_pandas(self, df: pd.DataFrame) -> pd.DataFrame:
        # 应用已拟合的转换器
        transformed_data = self.transformer.transform(df)
        
        # 获取转换后的列名
        new_cols = self.transformer.get_feature_names_out()
        
        # 创建新的 DataFrame
        transformed_df = pd.DataFrame(transformed_data, columns=new_cols, index=df.index)
        return transformed_df

    def __repr__(self):
        return (
            f"{self.__class__.__name__}(numeric_columns={self.numeric_columns!r}, "
            f"categorical_columns={self.categorical_columns!r})"
        )
