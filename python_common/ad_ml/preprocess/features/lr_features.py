
from pyspark.ml.feature import OneHotEncoder, StringIndexer,StringIndexerModel, VectorAssembler,StandardScaler,StandardScalerModel
from pyspark.ml import Pipeline
from pyspark.sql import functions
from python_common.ad_ml.preprocess.base_preprocess import TransformerMixin
from python_common.utils.logger import getLogger
logger = getLogger(__name__)

from python_common.ad_ml.preprocess.features.multi_onehot import MultiCategoryEncoder, MultiCategoryEncoderModel

class LRFeaturesTransfromer(TransformerMixin):
    def __init__(self,features_opts):
        self._features_opts = features_opts
        self._number_features,self._onehot_features,self._multi_onehot_features = self._features_list()
        self._df = None
        self._total_opts = [
            (self._onehot_features_opts,'onehot'),
            (self._multi_onehot_features_opts,'onehot'),
            (self._number_features_opts,'number')
        ]
    def _features_list(self):
        onehot_features = []
        number_features = []
        multi_onehot_features = []
        for k,v in self._features_opts.items():
            if 'number' in v:
                number_features.append(k)
            elif 'onehot' in v:
                onehot_features.append(k)
            elif 'multi_onehot' in v:
                multi_onehot_features.append(k)
        return number_features,onehot_features,multi_onehot_features

    def _onehot_features_opts(self):
        self._category_features_index = [f"{i}_idx" for i in self._apply_onehot_features]
        self._category_features_vec = [f"{i}_onehot_vec" for i in self._apply_onehot_features]
        self._category_features_indexer = StringIndexer(inputCols=self._apply_onehot_features, outputCols=self._category_features_index, handleInvalid='keep')
        self._category_features_encoder = OneHotEncoder(inputCols=self._category_features_index, outputCols=self._category_features_vec, dropLast=True)
        return [self._category_features_indexer,self._category_features_encoder],self._category_features_vec

    
    def _multi_onehot_features_opts(self):
        self._multi_category_features_vec = [f"{i}_onehot_vec" for i in self._multi_onehot_features]
        self._multi_category_features_encoder = [MultiCategoryEncoder(inputCol=i, outputCol=f'{i}_multi_onehot_vec') for i in self._multi_onehot_features]
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
        self._build_indexer()
        logger.info(self._vocabulary)
        return self
        

    def transform(self,df,y =None):
        return  self._model.transform(df)

    
    def _build_indexer(self):
        index = 0
        def _build_onehot_dict(index,name,opt, values):
            handled_values = ['' if v == 'nan' else v for v in values]
            index_mapper = {}
            for i in handled_values:
                index_mapper[i] = index
                index +=1
            return {
                'name': name,
                'opt':opt,
                'index':index_mapper
            }, index
        def _build_number_dict(index,name,mean=None,std=None):

            number_dict = {
                'name': name,
                'opt':'scaler',
                'index':index
            }
            if mean:
                number_dict['mean'] = mean
            if std:
                number_dict['std'] = std

            index +=1
         
            return number_dict,index

        vocabulary = []


        for m in self._model.stages:
            if isinstance(m,StringIndexerModel):
                input_cols_name = m.getInputCols()
                labels_arr = m.labelsArray
                for col,labels in zip(input_cols_name,labels_arr):
                    d,index = _build_onehot_dict(index, col,'one-hot',labels)
                    vocabulary.append(d)

            if isinstance(m, MultiCategoryEncoderModel):
                d,index = _build_onehot_dict(index,m.getInputCol(),'multi-one-hot', m.getVocabulary())
                vocabulary.append(d)
            
            if isinstance(m, StandardScalerModel):
                mean_arr = list(m.mean)
                std_arr = list(m.std)

                for col,mean,std in zip(self._number_features,mean_arr,std_arr):
                    d,index = _build_number_dict(index,col, mean,std)
                    vocabulary.append(d)

        self._vocabulary = vocabulary
        return vocabulary