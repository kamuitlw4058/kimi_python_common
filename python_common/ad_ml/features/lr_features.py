
from pyspark.ml.feature import OneHotEncoder, StringIndexer, VectorAssembler,StandardScaler
from pyspark.ml import Pipeline
from pyspark.sql import functions
from python_common.ad_ml.base import TransformerMixin
from python_common.utils.logger import getLogger
logger = getLogger(__name__)

from python_common.ad_ml.features.multi_onehot import MultiCategoryEncoder, MultiCategoryEncoderModel

class LRFeaturesTransfromer(TransformerMixin):
    def __init__(self,number_features=[],onehot_features=[],multi_onehot_features=[]):
        self._number_features = number_features
        self._onehot_features = onehot_features
        self._multi_onhot_features = multi_onehot_features
        self._df = None
        self._total_opts = [
            (self._onehot_features_opts,'onehot'),
            (self._multi_onehot_features_opts,'onehot'),
            (self._number_features_opts,'number')
        ]
    
    def _onehot_features_opts(self):
        self._category_features_index = [f"{i}_idx" for i in self._apply_onehot_features]
        self._category_features_vec = [f"{i}_onehot_vec" for i in self._apply_onehot_features]
        self._category_features_indexer = StringIndexer(inputCols=self._apply_onehot_features, outputCols=self._category_features_index, handleInvalid='keep')
        self._category_features_encoder = OneHotEncoder(inputCols=self._category_features_index, outputCols=self._category_features_vec, dropLast=True)
        return [self._category_features_indexer,self._category_features_encoder],self._category_features_vec

    
    def _multi_onehot_features_opts(self):
        self._multi_category_features_vec = [f"{i}_onehot_vec" for i in self._multi_onhot_features]
        self._multi_category_features_encoder = [MultiCategoryEncoder(inputCol=i, outputCol=f'{i}_multi_onehot_vec') for i in self._multi_onhot_features]
        return self._multi_category_features_encoder,self._multi_category_features_vec

    def _number_features_opts(self,mid_col = 'numbers',output_col='number_features'):
        if self._df is not None and len(self._number_features) > 0:
            opts =[]
            number_assembler = VectorAssembler(
                inputCols=self._number_features,
                outputCol=mid_col)
            opts.append(number_assembler)

            self._number_feaures_standscaler = StandardScaler(inputCol=mid_col,outputCol=output_col,withMean=True,withStd=True)
            opts.append(self._number_feaures_standscaler)
            return opts,[output_col]



    def fit(self,df,y=None, **fit_params):
        self._df = df
        self._apply_onehot_features = [i for i in self._onehot_features if df.agg(functions.countDistinct(df[i]).alias('cnt')).collect()[0].cnt > 1]
        logger.info(f' apply onehot features:{self._apply_onehot_features}')
        logger.info(f' apply number features:{self._number_features}')

        stages = []
        self._onehot_features_vec =[]
        self._number_features_col = None
        for opts,opt_type in self._total_opts:
            opt,output  = opts()
            if len(opt)>0:
                stages.extend(opt)
            if opt_type == 'onehot':
                self._onehot_features_vec.extend(output)
            elif opt_type == 'number':
                self._number_features_col = output

        if len(self._onehot_features_vec) > 0:
            self._cate_assembler = VectorAssembler(inputCols= self._onehot_features_vec, outputCol='onehot_features')
            stages.append(self._cate_assembler)
        

        self._pipeline = Pipeline(stages=stages)

        self._model = self._pipeline.fit(self._df)
        return self
        

    def transform(self,df,y =None):
        return  self._model.transform(df)