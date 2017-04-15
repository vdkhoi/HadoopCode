package de.l3s.local.word2vec.apply;

import org.deeplearning4j.models.embeddings.WeightLookupTable;
import org.deeplearning4j.models.embeddings.inmemory.InMemoryLookupTable;
import org.deeplearning4j.models.embeddings.loader.WordVectorSerializer;
import org.deeplearning4j.models.word2vec.VocabWord;
import org.deeplearning4j.models.word2vec.Word2Vec;
import org.deeplearning4j.models.word2vec.wordstore.inmemory.InMemoryLookupCache;
import org.deeplearning4j.text.sentenceiterator.BasicLineIterator;
import org.deeplearning4j.text.sentenceiterator.SentenceIterator;
import org.deeplearning4j.text.tokenization.tokenizer.preprocessor.CommonPreprocessor;
import org.deeplearning4j.text.tokenization.tokenizerfactory.DefaultTokenizerFactory;
import org.deeplearning4j.text.tokenization.tokenizerfactory.TokenizerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Collection;

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
 * java -cp /home/khoi/jars/dl4j_word2vec-1.0.0-SNAPSHOT-jar-with-dependencies.jar de.l3s.archive.anchor.word2vec.applymodel.dl4j_build_vec /home/khoi/follow_up_websci_task/data/Word2VecModel /home/khoi/follow_up_websci_task/data/Word2VecVectors
 * 
 */

public class dl4j_build_vec {

	private static Logger log = LoggerFactory
			.getLogger(dl4j_build_vec.class);

	public static void main(String[] args) throws Exception {
		/*
		 * Initial model training phase
		 */
		
		String modelPath = "Z:/Alexandria/Results/Result_follow_up_WebSci16/Word2VecModel.txt";
		String vectorPath = "Z:/Alexandria/Results/Result_follow_up_WebSci16/Word2VecModel.txt";
		
		if (args.length == 2) {
			modelPath = args[0];
			vectorPath = args[1];
		}
		
		Word2Vec word2Vec = WordVectorSerializer
				.loadFullModel(modelPath);

		WordVectorSerializer.writeWordVectors(word2Vec, new File(vectorPath));
	}
}
