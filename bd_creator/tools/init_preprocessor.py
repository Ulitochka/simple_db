

class InitialPreprocessor:
    def __init__(self, max_text_len):
        self._max_text_len = max_text_len

    def validate_row(self, row):
        mark = True
        empty_elements = [el for el in row if el == '']
        miss_elements = False if len(row) == 7 else True
        non_valid_text_len = True if len(row[1]) > self._max_text_len else False

        if empty_elements:
            mark = False
        elif miss_elements:
            mark = False
        elif non_valid_text_len:
            mark = False

        return mark

    def normalize_row(self, row):
        return [el.replace('\t', '').replace('\\', '') for el in row]
