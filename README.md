# LearningToRank
Ranking long tail queries project
https://www.kaggle.com/c/ranking-long-tail-queries-fall-2020/overview

# Воспроизведение
## Получение кликовых фичей из логов
0) Положите все файлы из FeatureExtraction/Data/ в hadoop://user/andrey.moskalenko, а так же распакованный архив 2017
2) Перейти в /Hadoop/
3) ```gradle build```
4) Run: ```hadoop jar final.jar MainJob 2017 url_features url.data queries.tsv train.marks.tsv sample.csv url```
5) Run: ```hadoop jar final.jar MainJob 2017 host_features host.data queries.tsv train_host.tsv test_host.csv host```
6) Полученные фичи положите в Prediction/url_my и Prediction/host_my

## Текстовые фичи
### Заголовки
1) Перейдите в FeatureExtraction/
2) Последовательно выполните все ячейки следующих ноутбуков следуя комментариям в них:
3) Препроцессинг тайтлов + BM25 + TFIDF: **Titles_Text_preprocessing_BM25_TFIDF.ipynb**
4) Препроцессинг тайтлов + BM25 + TFIDF: **Titles_Text_preprocessing_BM25_TFIDF.ipynb**
5) Universal Sentence Encoder:  **Titles_USE.ipynb**
6) Fasstext:  **Titles_Fasttext.ipynb**. Для корректного запуска загрузите следующие модели и поместите рядом с ноутбуком: 
https://dl.fbaipublicfiles.com/fasttext/vectors-crawl/cc.ru.300.bin.gz
http://files.deeppavlov.ai/embeddings/ft_native_300_ru_wiki_lenta_lower_case/ft_native_300_ru_wiki_lenta_lower_case.bin

### Тексты
1) Перейдите в FeatureExtraction/
2) Последовательно выполните все ячейки следующих ноутбуков следуя комментариям в них:
3) Препроцессинг текстов: **Full_texts_preproc.ipynb**
4) BM25 на первых 512 словах: **BM25_512words.ipynb**
5) Universal Sentence Encoder + LaReQA:  **Full_texts_USEQA_LaReQA.ipynb**

Перенесите полученные в этой фазе папки фичей в /Prediction. Получится структура как Prediction/BM25, Prediction/LaReQA, Prediction/TFIDF и т.д.

## Предсказание
1) Перейдите в Prediction/
2) Последовательно выполните все ячейки следующих ноутбуков следуя комментариям в них:
3) Получение финального предсказания и графика полезности фичей: **Predict.ipynb**

Скор public: **0.78536**, private: **0.78337**
