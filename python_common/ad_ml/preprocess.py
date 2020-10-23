import abc

from python_common.utils.logger import getLogger
logger = getLogger(__name__)



class  NegativeSampling():
    # MAX_POS_SAMPLE = 666 * 10e3
    # MIN_POS_SAMPLE = 40 * 10e3
    def __init__(self,pos_proportion,neg_proportion,clk_name,imp_name,max_pos_sample= 666 * 10e3):
        self._clk_name = clk_name
        self._imp_name = imp_name
        self._max_pos_sample  = max_pos_sample
        self._pos_proportion = pos_proportion
        self._neg_proportion = neg_proportion
        self._pos_ratio = None
        self._neg_ratio = None
    
    def _sample_ratio(self,clk_count,imp_count):
        if clk_count > self._max_pos_sample:
            clk_count = self._max_pos_sample

        pos_ratio = self._pos_proportion
        neg_ratio = self._neg_proportion * clk_count / (imp_count - clk_count)

        pos_ratio = min(pos_ratio, 1)
        neg_ratio = min(neg_ratio, 1)

        return pos_ratio, neg_ratio

    def fit_transform(self,df,clk_count,imp_count):
        self._pos_ratio, self._neg_ratio = self._sample_ratio(clk_count,imp_count)
        pos_df = df.filter(f"{self._clk_name} > 0").sample(self._pos_ratio)
        neg_df = df.filter(f"{self._clk_name} = 0").sample(self._neg_ratio)
        return pos_df.union(neg_df)

    
    def transform(self,df):
        if not self._neg_ratio or not self._pos_ratio:
            raise ValueError('before transform fit first!!')

        pos_df = df.filter(f"{self._clk_name} > 0").sample(self._pos_ratio)
        neg_df = df.filter(f"{self._clk_name} = 0").sample(self._neg_ratio)
        return pos_df.union(neg_df)