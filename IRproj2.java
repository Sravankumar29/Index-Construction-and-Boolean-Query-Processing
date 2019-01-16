package INFRproject2.INFproject2;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.MultiFields;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.store.FSDirectory;

public class IRproj2 {

	/************** GetPosting lists ****************/
	public static void getPostings(String[] query, Map<String, LinkedList<Integer>> dictionary, BufferedWriter bwout)
			throws IOException {
		for (String term : query) {
			bwout.write("GetPostings\r\n");
			bwout.write(term + "\r\n");
			bwout.write("Postings list: ");
			for (int i : dictionary.get(term)) {
				bwout.write(i + " ");
			}
			bwout.write("\r\n");
		}
	}

	/*************** TaatOr Operation ****************/

	public static void taatOR(String[] query, Map<String, LinkedList<Integer>> dictionary, BufferedWriter bwout)
			throws IOException {
		int taatOrcomparisons = 0;
		bwout.write("TaatOr\r\n");
		String res = "";
		for (String str : query) {
			res = res + str + " ";
		}
		bwout.write(res + "\r\n");
		bwout.write("Results: ");
		LinkedList<Integer> result = dictionary.get(query[0]);
		for (int t = 1; t < query.length; t++) {
			LinkedList<Integer> termpostings = dictionary.get(query[t]);
			LinkedList<Integer> mergepostings = new LinkedList<Integer>();
			int i = 0, j = 0;
			if (termpostings.equals(null)) {
				continue;
			}
			if (result.equals(null)) {
				result = termpostings;
			}
			while (i < result.size() && j < termpostings.size()) {
				if (result.get(i) < termpostings.get(j)) {
					taatOrcomparisons++;
					mergepostings.add(result.get(i));
					i++;
				} else if (result.get(i).equals(termpostings.get(j))) {
					taatOrcomparisons++;
					mergepostings.add(result.get(i));
					i++;
					j++;
				} else {
					taatOrcomparisons++;
					mergepostings.add(termpostings.get(j));
					j++;
				}
			}
			while (i < result.size()) {
				mergepostings.add(result.get(i));
				i++;
			}
			while (j < termpostings.size()) {
				mergepostings.add(termpostings.get(j));
				j++;
			}
			result = mergepostings;
		}
		if (result.size() == 0) {
			bwout.write("empty");
		} else {
			for (Integer i : result) {
				bwout.write(i + " ");
			}
		}
		bwout.write("\r\nNumber of documents in results: " + result.size() + "\r\n");
		bwout.write("Number of comparisons: " + taatOrcomparisons + "\r\n");
	}

	/*********************** TaatAnd Operation ******************************/

	public static void taatAND(String[] query, Map<String, LinkedList<Integer>> dictionary, BufferedWriter bwout)
			throws IOException {
		int tAndcomparisons = 0;
		LinkedList<Integer> result = dictionary.get(query[0]);
		for (int t = 1; t < query.length; t++)
		{
		    LinkedList<Integer> termpost = dictionary.get(query[t]);
		    LinkedList<Integer> merge = new LinkedList<Integer>();
		    int i = 0, j = 0;
		    if (termpost.size() == 0 || result.size() == 0)
		    {
		break;
		    }
		    while (i < result.size() && j < termpost.size())
		    {
		if (result.get(i) < termpost.get(j))
		{
		    int skiplength=(int)Math.sqrt(result.size());
		    tAndcomparisons++;
		    if((i+skiplength)<(result.size()) &&
		result.get(i+skiplength)<=termpost.get(j))
		    {//skip pointers
		   tAndcomparisons++;
		   i=i+skiplength;
		   System.out.println("skipped");
		  while((i+skiplength)<(result.size()) &&
		result.get(i+skiplength)<=termpost.get(j))
		  { tAndcomparisons++;
		      i=i+skiplength;

		  }
		    }
		    else {
		    tAndcomparisons++;
		    i++;
		    }
		}
		else if (result.get(i).equals(termpost.get(j)))
		{
		    tAndcomparisons++;
		    merge.add(result.get(i));
		    i++;
		    j++;
		}
		else
		{
		    int skiplength=(int)Math.sqrt(termpost.size());
		    if((j+skiplength)<(termpost.size()) &&
		termpost.get(j+skiplength)<=result.get(i))
		    {
		   tAndcomparisons++;
		   j=j+skiplength;
		  while((j+skiplength)<(termpost.size()) &&
		termpost.get(j+skiplength)<=result.get(i))
		  { tAndcomparisons++;
		      j=j+skiplength;
		  }
		    }//skip pointers
		    else {
		    tAndcomparisons++;
		    j++;
		    }

		}
		    }
		    result = merge;
		}
		bwout.write("TaatAnd\r\n");
		String tm = "";
		for (String s : query)
		{
		    tm = tm + s + " ";
		}
		bwout.write(tm + "\r\n");
		bwout.write("Results: ");
		if (result.size() == 0)
		{
		    bwout.write("empty");
		    bwout.write("\r\nNumber of documents in results: " + 0 + "\r\n");
		}
		else
		{
		    for (Integer i : result)
		    {
		bwout.write(i + " ");
		    }
		    bwout.write("\r\nNumber of documents in results: " + result.size() + "\r\n");
		}
		bwout.write("Number of comparisons: " + tAndcomparisons + "\r\n");	}

	/****************** DaatAnd Operation ***************/

	public static void daatAnd(String[] query, Map<String, LinkedList<Integer>> index, BufferedWriter bwout)
			throws IOException {
		int daatAndCount = 0;
		bwout.write("DaatAnd\r\n");
		String res = "";
		for (String str : query) {
			res = res + str + " ";
		}
		bwout.write(res + "\r\n");
		bwout.write("Results: ");
		ArrayList<LinkedList<Integer>> queryterms = new ArrayList<LinkedList<Integer>>();
		LinkedList<Integer> postings = new LinkedList<Integer>();
		LinkedList<Integer> ref = new LinkedList<Integer>();
		LinkedList<Integer> result = new LinkedList<Integer>();
		int[] postingSize = new int[query.length];
		for (int i = 0; i < query.length; i++) {
			postings = index.get(query[i]);
			queryterms.add(postings);
			postingSize[i] = queryterms.get(i).size();
		}
		int minSize = postingSize[0];
		int minPosting = 0;
		for (int i = 1; i < query.length; i++) {
			if (postingSize[i] < minSize) {
				minSize = postingSize[i];
				minPosting = i;
			}
		}
		ref = queryterms.get(minPosting);
		queryterms.add(0, ref);
		queryterms.remove(minPosting + 1);
		queryterms.sort(new Comparator<LinkedList<Integer>>() {
			public int compare(LinkedList<Integer> o1, LinkedList<Integer> o2) {
				if (o1.size() < o2.size())
					return -1;
				else
					return 1;
			}
		});
		int count = 1;
		int temp_index = 0;
		for (int i = 0; i < queryterms.get(0).size(); i++) {
			count = 1;
			for (int j = 1; j < queryterms.size(); j++) {
				int flag = 0;
				for (int k = temp_index; k < queryterms.get(j).size(); k++) {
					daatAndCount++;
					if (queryterms.get(0).get(i).equals(queryterms.get(j).get(k))) {
						count++;
						break;
					} else if (queryterms.get(0).get(i) < (queryterms.get(j).get(k))) {
						temp_index = k;
						flag = 1;
						break;
					}
				}
				if (flag == 1)
					break;
			}
			if (count == queryterms.size()) {
				result.add(queryterms.get(0).get(i));
			}
		}
		for (int i = 0; i < result.size(); i++) {
			bwout.write(result.get(i) + " ");
		}
		if (result.size() == 0) {
			bwout.write("empty");
		}
		bwout.write("\r\nNumber of documents in results: " + result.size() + "\r\n");
		bwout.write("Number of comparisons: " + daatAndCount + "\r\n");

	}

	/*********************** DaatOr Opeartion **************/

	public static void daatOR(String[] query, Map<String, LinkedList<Integer>> dictionary, BufferedWriter bwout)
			throws IOException {
		int daatOrcomparisons = 0;
		bwout.write("DaatOr\r\n");
		String res = "";
		for (String str : query) {
			res = res + str + " ";
		}
		bwout.write(res + "\r\n");
		bwout.write("Results: ");
		ArrayList<LinkedList<Integer>> queryterms = new ArrayList<LinkedList<Integer>>();
		LinkedList<Integer> postings = new LinkedList<Integer>();
		LinkedList<Integer> ref = new LinkedList<Integer>();
		LinkedList<Integer> result = new LinkedList<Integer>();
		int z = 0;
		int[] postingSize = new int[query.length];
		for (int i = 0; i < query.length; i++) {
			postings = dictionary.get(query[i]);
			queryterms.add(postings);
			postingSize[i] = queryterms.get(i).size();
		}
		int maxSize = postingSize[0];
		int maxPosting = 0;
		for (int i = 1; i < query.length; i++) {
			if (postingSize[i] > maxSize) {
				maxSize = postingSize[i];
				maxPosting = i;
			}
		}
		ref = queryterms.get(maxPosting);
		queryterms.add(0, ref);
		queryterms.remove(maxPosting + 1);
		int size = queryterms.size();
		int[] Pointers = new int[size];
		int[] Sizes = new int[size];
		for (int i = 0; i < queryterms.size(); i++) {
			Sizes[i] = queryterms.get(i).size();
			Pointers[i] = 0;
		}
		boolean end = false;
		while (end == false && queryterms.size() != 1) {
			int counter = 0;
			for (int i = 0; i < size; i++) {
				if (Pointers[i] == Sizes[i])
					counter++;
			}
			if (counter == size)
				end = true;

			if (end == false) {
				int j = 0;
				while (j < size) {
					if (Pointers[j] < Sizes[j]) {
						long a = queryterms.get(j).get(Pointers[j]);
						boolean flag = true;
						daatOrcomparisons++;
						for (int i = 0; i < result.size(); i++) {
							if (a == result.get(i)) {
								flag = false;
								Pointers[j]++;
							}
						}
						if (flag == true) {
							result.add(queryterms.get(j).get(Pointers[j]));
							Pointers[j]++;
						}
					}
					j++;
				}
			}
		}
		while (queryterms.size() == 1 && z < queryterms.get(0).size()) {
			result.add(queryterms.get(0).get(z));
			z = z + 1;
		}
		Collections.sort(result);
		for (int i = 0; i < result.size(); i++) {
			bwout.write(result.get(i) + " ");
		}
		if (result.size() == 0) {
			bwout.write("empty");
		}
		bwout.write("\r\nNumber of documents in results: " + result.size() + "\r\n");
		bwout.write("Number of comparisons: " + daatOrcomparisons + "\r\n");
	}

	/************************ Main Method ***********************/

	public static void main(String[] args) throws IOException {

		Map<String, LinkedList<Integer>> dictionary = new HashMap<String, LinkedList<Integer>>();
		FileSystem fs = FileSystems.getDefault();

		String indexpath = args[0];
		String outputfile = args[1];
		String inputfile = args[2];
		BufferedReader brin = new BufferedReader(
				new InputStreamReader(new FileInputStream(new File(inputfile)), "UTF-8"));
		BufferedWriter bwout = new BufferedWriter(
				new OutputStreamWriter(new FileOutputStream(new File(outputfile)), "UTF-8"));
		Path pathobj = fs.getPath(indexpath);
		IndexReader ir = DirectoryReader.open(FSDirectory.open(pathobj));
		Collection<String> docs = MultiFields.getIndexedFields(ir);
		Iterator<String> itr = docs.iterator();
		Terms terms;
		TermsEnum termsiterator;
		PostingsEnum postingsiterator = null;
		ArrayList<String> stringlist = new ArrayList<String>();
		while (itr.hasNext()) {
			String element = itr.next();
			if (element.equals("id") || element.equals("_version_")) {
				continue;
			}
			terms = MultiFields.getTerms(ir, element);
			termsiterator = terms.iterator();
			while (termsiterator.next() != null) {
				stringlist.add(termsiterator.term().utf8ToString());
				postingsiterator = MultiFields.getTermDocsEnum(ir, element, termsiterator.term());
				LinkedList<Integer> ll = new LinkedList<Integer>();
				while (postingsiterator.nextDoc() != PostingsEnum.NO_MORE_DOCS) {
					ll.add(postingsiterator.docID());
				}
				dictionary.put(termsiterator.term().utf8ToString(), ll);
			}
		}
		String currentline;
		while ((currentline = brin.readLine()) != null) {
			String[] queryterms = currentline.split(" ");
			getPostings(queryterms, dictionary, bwout);
			taatAND(queryterms, dictionary, bwout);
			taatOR(queryterms, dictionary, bwout);
			daatAnd(queryterms, dictionary, bwout);
			daatOR(queryterms, dictionary, bwout);
		}
		bwout.close();
		brin.close();
	}
}
