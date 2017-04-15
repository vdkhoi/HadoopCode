package de.l3s.hadoop.mapreduce.feature.word2vec.learn;

import java.io.FileOutputStream;
import java.util.zip.GZIPOutputStream;

import org.deeplearning4j.models.embeddings.WeightLookupTable;
import org.deeplearning4j.models.embeddings.inmemory.InMemoryLookupTable;
import org.deeplearning4j.models.embeddings.loader.WordVectorSerializer;
import org.deeplearning4j.models.word2vec.VocabWord;
import org.deeplearning4j.models.word2vec.Word2Vec;
import org.deeplearning4j.models.word2vec.wordstore.inmemory.InMemoryLookupCache;
import org.deeplearning4j.text.sentenceiterator.BasicLineIterator;
import org.deeplearning4j.text.sentenceiterator.SentenceIterator;
import org.deeplearning4j.text.tokenization.tokenizer.preprocessor.CustomStemmingPreprocessor;
import org.deeplearning4j.text.tokenization.tokenizerfactory.DefaultTokenizerFactory;
import org.deeplearning4j.text.tokenization.tokenizerfactory.TokenizerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tartarus.snowball.ext.GermanStemmer;




/**
 * This is simple example for model weights update after initial vocab building.
 * If you have built your w2v model, and some time later you've decided that it
 * can be additionally trained over new corpus, here's an example how to do it.
 *
 * PLEASE NOTE: At this moment, no new words will be added to vocabulary/model.
 * Only weights update process will be issued. It's often called
 * "frozen vocab training".
 *
 * @author raver119@gmail.com
 */


/*
 * Cluster command:
 * 
 * java -cp /home/khoi/jars/dl4j_word2vec-1.0.0-SNAPSHOT-jar-with-dependencies.jar de.l3s.archive.anchor.word2vec.training.dl4j_WordModelTraining /home/khoi/follow_up_websci_task/data/ anchors Word2Vec_VectorGerman 180
 */

public class dl4j_WordModelTraining {

	private static Logger log = LoggerFactory
			.getLogger(dl4j_WordModelTraining.class);

	public static void main(String[] args) throws Exception {
		/*
		 * Initial model training phase
		 */
		
		String filePath = "Z:/Alexandria/Results/Result_follow_up_WebSci16/200_entities_list.txt";
		String vector_path = "Z:/Alexandria/Results/Result_follow_up_WebSci16/Word2Vec_ModelGerman.txt";
		int frequency_cutoff = 1;
		String word = "angela";
		
		if (args.length == 4) {
			filePath = args[0] + "/" + args[1];
			vector_path = args[0] + "/" + args[2] + ".gz";
			frequency_cutoff = Integer.parseInt(args[3]);
		}
		
		//String filePath = new ClassPathResource("raw_sentences.txt").getFile().getAbsolutePath();

		log.info("Load & Vectorize Sentences....");
		// Strip white space before and after for each line
		SentenceIterator iter = new BasicLineIterator(filePath);
		// Split on white spaces in the line to get words
		TokenizerFactory t = new DefaultTokenizerFactory();
		
		
		t.setTokenPreProcessor(new CustomStemmingPreprocessor(new GermanStemmer()));

		// manual creation of VocabCache and WeightLookupTable usually isn't
		// necessary
		// but in this case we'll need them
		InMemoryLookupCache cache = new InMemoryLookupCache();
		WeightLookupTable<VocabWord> table = new InMemoryLookupTable.Builder<VocabWord>()
				.vectorLength(100).useAdaGrad(false).cache(cache).lr(0.025f)
				.build();

		log.info("Building model....");
		Word2Vec vec = new Word2Vec.Builder().minWordFrequency(frequency_cutoff).iterations(1)
				.epochs(1).layerSize(100).seed(42).windowSize(50).iterate(iter)
				.tokenizerFactory(t).lookupTable(table).vocabCache(cache)
				.build();

		vec.fit();


		/*
		 * at this moment we're supposed to have model built, and it can be
		 * saved for future use.
		 */
		WordVectorSerializer.writeWordVectors(vec, new GZIPOutputStream(new FileOutputStream(vector_path)));
	}
}
