from collections import Counter

def test_no_duplicates_in_symbols_tbTradingHistoric():
    # Simular los valores para symbols_tbtickers, symbols_tbgidsdirectory symbols_tbdimSymbols y symbols_tbTradingHistoric
    symbols_tbtickers = [('AAPL',), ('GOOG',), ('MSFT',)]
    symbols_tbgidsdirectory = [('AMZN',), ('TSLA',)]
    symbols_tbdimSymbols = [('GOOG',), ('AAPL',), ('FB',)]
    symbols_tbTradingHistoric = [('AAPL',), ('GOOG',), ('AAPL',), ('TSLA',), ('MSFT',)]

    # Combinar los símbolos de tbtickers y tbGIDSDirectory
    combined_symbols = symbols_tbtickers + symbols_tbgidsdirectory + symbols_tbTradingHistoric

    # Obtener los símbolos únicos de la combinación y tbTradingHistoric, excluyendo symbols_tbdimSymbols
    combined_symbols = list(set([symbol[0] for symbol in combined_symbols if symbol not in symbols_tbdimSymbols]))

    # Contar las ocurrencias de cada símbolo
    symbol_counts = Counter(symbol[0] for symbol in combined_symbols)

    # Verificar si hay valores duplicados
    assert all(count == 1 for count in symbol_counts.values()), "La lista symbols_tbTradingHistoric contiene duplicados."
