# src/training/model.py
import torch
import torch.nn as nn

class SimpleRecommendationModel(nn.Module):
    """
    一个简单的 PyTorch 模型示例。
    可以根据你的 `model_params.yaml` 文件进行配置。
    """
    def __init__(self, input_features: int, config: dict):
        super(SimpleRecommendationModel, self).__init__()
        
        layers = []
        hidden_layers = config.get("hidden_layers", [128, 64])
        dropout_rate = config.get("dropout", 0.3)
        
        # 输入层
        layers.append(nn.Linear(input_features, hidden_layers[0]))
        layers.append(nn.ReLU())
        layers.append(nn.Dropout(dropout_rate))
        
        # 隐藏层
        for i in range(len(hidden_layers) - 1):
            layers.append(nn.Linear(hidden_layers[i], hidden_layers[i+1]))
            layers.append(nn.ReLU())
            layers.append(nn.Dropout(dropout_rate))
            
        # 输出层 (例如，二分类问题)
        layers.append(nn.Linear(hidden_layers[-1], 1))
        
        self.model = nn.Sequential(*layers)

    def forward(self, x):
        return self.model(x)
