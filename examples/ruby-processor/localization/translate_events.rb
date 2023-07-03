def init(params)
    require 'i18n'
    I18n.load_path << params["locale_map_path"]
    I18n.default_locale = params["locale"]
end

def process(event)
    message = event.get("item")
    translation = I18n.t(message, default: 'translation not found')
    event.put("translated_item", translation)
end