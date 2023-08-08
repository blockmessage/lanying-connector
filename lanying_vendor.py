import lanying_vendor_openai
import lanying_vendor_minimax
import lanying_vendor_baidu
import lanying_vendor_zhipuai
import lanying_vendor_azure

vendor_to_module = {
    'openai': lanying_vendor_openai,
    'minimax': lanying_vendor_minimax,
    'baidu': lanying_vendor_baidu,
    'zhipuai': lanying_vendor_zhipuai,
    "azure": lanying_vendor_azure
}
def get_module(vendor):
    return vendor_to_module.get(vendor)

def get_chat_model_config(vendor, model):
    module = get_module(vendor)
    if module:
        model_configs = module.model_configs()
        for config in model_configs:
            if config['type'] == "chat":
                is_prefix = config.get('is_prefix', True)
                now_model = config.get('model')
                if is_prefix and model.startswith(now_model):
                    return config
                if model == now_model:
                    return config
    return None

def get_embedding_model_config(vendor, model):
    module = get_module(vendor)
    if module:
        model_configs = module.model_configs()
        for config in model_configs:
            if config['type'] == "embedding":
                is_prefix = config.get('is_prefix', True)
                now_model = config.get('model')
                if is_prefix and model.startswith(now_model):
                    return config
                if model == now_model:
                    return config
    return None

def prepare_chat(vendor, auth_info, preset):
    module = get_module(vendor)
    return module.prepare_chat(auth_info, preset)

def chat(vendor, prepare_info, preset):
    module = get_module(vendor)
    return module.chat(prepare_info, preset)

def prepare_embedding(vendor, auth_info, type):
    module = get_module(vendor)
    return module.prepare_embedding(auth_info, type)

def embedding(vendor, prepare_info, text):
    module = get_module(vendor)
    return module.embedding(prepare_info, text)

def encoding_for_model(vendor, model):
    module = get_module(vendor)
    return module.encoding_for_model(model)
