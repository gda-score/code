import pandas


class DupCheck:
    def __init__(self):
        self._data = pandas.DataFrame()
        pandas.set_option('display.max_rows', None)
        pandas.set_option('display.max_columns', None)
        pandas.set_option('display.width', None)
        pandas.set_option('display.max_colwidth', None)

    def is_claimed(self, spec, verbose=False, raise_true=False):
        if verbose:
            print(f"DupCheck: Calling is_claimed with spec {spec}")
        DupCheck._check_cols(spec)
        if self._data.empty:
            if verbose:
                print(f"DupCheck: [NEW] Nothing claimed yet.")
            return False
        remaining = list(self._data.columns)
        none_match = DupCheck._match(spec, self._data, remaining, none_matches=True)
        if none_match.empty:
            if verbose:
                print(f"DupCheck: [NEW] Claim does not match any previous claim.")
            return False
        match = DupCheck._match(spec, none_match, remaining, none_matches=False)
        if match.empty:
            message = f"DupCheck: [DUPLICATE] Claim already partially made. Must be combined with previous claim:\n" + \
                      f"----------\n{none_match}\n----------"
            if verbose:
                print(message)
            if raise_true:
                raise ValueError(message)
            return True
        message = f"DupCheck: [DUPLICATE] Data of claim is fully contained in the following previous claim(s):\n" + \
                  f"----------\n{match}\n----------"
        if verbose:
            print(message)
        if raise_true:
            raise ValueError(message)
        return True

    def claim(self, spec, verbose=False):
        if verbose:
            print(f"DupCheck: Calling claim with spec {spec}")
        _ = self.is_claimed(spec, verbose=verbose, raise_true=True)
        if verbose:
            print(f"DupCheck: Storing claim with spec {spec}")
        self._insert_row(spec)

    def _insert_row(self, spec):
        data_lst = []
        data_lst += spec['known'] if 'known' in spec and spec['known'] else []
        data_lst += spec['guess'] if 'guess' in spec and spec['guess'] else []
        self._create_missing_columns(data_lst)
        val_lst = []
        for col in self._data.columns:
            val = DupCheck._find_val(col, data_lst)
            val_lst.append(val)
        self._data.loc[len(self._data)] = val_lst

    def _create_missing_columns(self, lst):
        for dct in lst:
            if 'col' not in dct:
                raise ValueError(f"key 'col' not found in partial spec: {dct}")
            col = dct['col']
            if col in self._data.columns:
                continue
            self._data[col] = None

    @staticmethod
    def _find_val(col, lst):
        for dct in lst:
            if 'col' not in dct:
                raise ValueError(f"key 'col' not found in partial spec: {dct}")
            if dct['col'] == col:
                if 'val' not in dct:
                    raise ValueError(f"key 'val' not found in partial spec: {dct}")
                return dct['val']
        return None

    @staticmethod
    def _match(spec, data, remaining_cols, none_matches=True):
        val_lst = []
        val_lst += spec['known'] if 'known' in spec and spec['known'] else []
        val_lst += spec['guess'] if 'guess' in spec and spec['guess'] else []
        return DupCheck._match_step(val_lst, 0, data, remaining_cols, none_matches=none_matches)

    @staticmethod
    def _match_step(val_lst, idx, data, remaining_cols, none_matches=True):
        if remaining_cols is None or not remaining_cols:
            return data
        if len(val_lst) <= idx:
            return DupCheck._match_step([{'col': c, 'val': None} for c in remaining_cols], 0, data, remaining_cols,
                                        none_matches=none_matches)
        val_dct = val_lst[idx]
        if 'col' not in val_dct:
            raise ValueError(f"key 'col' not found in partial spec: {val_dct}")
        col = val_dct['col']
        if 'val' not in val_dct:
            raise ValueError(f"key 'val' not found in partial spec: {val_dct}")
        val = val_dct['val']
        if col not in data.columns:
            return pandas.DataFrame()
        remaining_cols.remove(col)
        if none_matches:
            loc = data.loc[(data[col] == val) | (data[col].isnull())]
        else:
            if val is None:
                loc = data.loc[data[col].isnull()]
            else:
                loc = data.loc[data[col] == val]
        return DupCheck._match_step(val_lst, idx + 1, loc, remaining_cols, none_matches=none_matches)

    @staticmethod
    def _cols_of_lst(lst):
        cols = set()
        for col_dct in lst:
            if 'col' not in col_dct:
                raise ValueError(f"key 'col' not found in partial spec: {col_dct}")
            col = col_dct['col']
            if col in cols:
                raise ValueError(f"DupCheck: Column {col} cannot occur twice.")
            cols.add(col)
        return cols

    @staticmethod
    def _check_cols(spec):
        known = DupCheck._cols_of_lst(spec['known']) if 'known' in spec and spec['known'] else set()
        guess = DupCheck._cols_of_lst(spec['guess']) if 'guess' in spec and spec['guess'] else set()
        intersection = known.intersection(guess)
        if intersection:
            raise ValueError(f"DupCheck: Columns {intersection} cannot occur in both 'known' and 'guess'.")
