# Data cleaning constants
SPACES_REGEX = r"\s+"
# Segunda lista é o padrão para missings de read_csv do pandas
MISSING_VALUES = ["", "N.A.", "<Null>"] + [" ", "#N/A", "#N/A N/A", "#NA", "-1.#IND", "-1.#QNAN", "-NaN", "-nan", "1.#IND", "1.#QNAN", "<NA>", "N/A", "NA", "NULL", "NaN", "None", "n/a", "nan", "null"]
NORMALIZE_VALUES = {
    ',': "."
}


# Funções para Pipes
def handle_missing(series, extra_missing_values=[]):
    return (
        series.str.replace(SPACES_REGEX, "", regex=True)
            .replace(MISSING_VALUES + extra_missing_values, None) 
    )

def handle_normalized(series, extra_replaces={}):
    extra_replaces.update(NORMALIZE_VALUES)
    
    for key, value in extra_replaces.items():
        series = series.str.replace(key, value)
    return series