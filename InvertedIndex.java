/**
* @author Akshay
*/
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.MultiFields;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.BytesRef;

public class InvertedIndex {
	
	static LinkedList<Integer> mList = null;
	static HashMap<String, LinkedList<Integer>> mHashMap = new HashMap<String, LinkedList<Integer>>();
	public static void main(String[] args) {		
		GeneratePostingsList(args);
		ExecuteQuery(args);
//		System.out.println("done !!");
	}

	/** This method generates postingsList and stores in Hashmap <string(term), linkedlist(postings of terms)>
	 *  args[0] contains path to index
	 * @param args command line argument
	 */
	private static void GeneratePostingsList(String[] args) {
		File IndexFile = new File(args[0]); 
		try {
			Directory directory = FSDirectory.open(IndexFile.toPath());
			IndexReader reader = DirectoryReader.open(directory);
			Fields fields = MultiFields.getFields(reader);
			ArrayList<String> mFieldList = new ArrayList<String>();
			for (String field : fields) {
				if(field.equals("id") || field.equals("_version_"))
	        		continue;
				mFieldList.add(field);
			}
	        for (String field : mFieldList) {
	            Terms terms = fields.terms(field);
	            TermsEnum termsEnum = terms.iterator();
	            BytesRef byteRef = null;
	            int flag = 0;
	            while ((byteRef = termsEnum.next()) != null) {
	                mList = null;
	                String s = byteRef.utf8ToString();
	                if(!mHashMap.containsKey(s))
	                	mHashMap.put(s, mList);
	                mList = mHashMap.get(s);
	                if(mList == null)
	                	mList =new LinkedList<Integer>();
	                else
	                	flag = 1;
	                PostingsEnum pEnum = null;
	                pEnum = termsEnum.postings(pEnum);
	                int docId = pEnum.nextDoc();
	                while(docId != PostingsEnum.NO_MORE_DOCS) {
	                	mList.add(docId);
	                	docId = pEnum.nextDoc();
	                }	  
	                if(flag == 1)
	                	Collections.sort(mList);
	                flag = 0;
	                mHashMap.put(s, mList);
	            }
	        }
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * This method reads query terms line by line, performs TAAT_OR, TAAT_AND, DAAT_OR, DAAT_AND and stores the result in output.txt
	 * @param args args[1] contains path to output.txt
	 * args[2] contains path to input.txt
	 */
	private static void ExecuteQuery(String[] args) {
		try {
			BufferedReader br = new BufferedReader(new InputStreamReader( new FileInputStream(args[2]), "UTF-8"));
			Writer writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(args[1]), "utf-8"));
			LinkedList<Integer> mListTaaT1 = null,mListTaaT2 = null, temp = null;
			LinkedList<Integer> mListTaatResultAND = null, mListTaatResultOR = null;
			LinkedList<Integer> mListDaatResultAND = null, mListDaatResultOR = null;
			int countTAATAND, countTAATOR, countDAATAND, countDAATOR; 
			String line ="";
			while((line = br.readLine()) != null) {				
				String[] queryTerm = line.split(" ");
				countTAATAND = countTAATOR = countDAATAND = countDAATOR = 0;
				for(int i=0;i<queryTerm.length;i++) {
					writer.write("GetPostings");writer.write('\n');
					writer.write(queryTerm[i]);writer.write('\n');
					temp = mHashMap.get(queryTerm[i]);
					writer.write("Postings list:");
					for(int j=0;j<temp.size();j++)
						writer.write(" "+temp.get(j));
					writer.write('\n');
				}
				mListTaatResultAND = mHashMap.get(queryTerm[0]);
				mListTaatResultOR = mHashMap.get(queryTerm[0]);
				for(int i=1;i<queryTerm.length;i++) {
					mListTaaT1 = mListTaatResultAND; 
					mListTaaT2 = mHashMap.get(queryTerm[i]);
					
					mListTaatResultAND=null;
					mListTaatResultAND = new LinkedList<Integer>();
					countTAATAND += getIntersectionForTaaT(mListTaaT1, mListTaaT2, mListTaatResultAND);
					
					mListTaaT1 = mListTaatResultOR;
					mListTaatResultOR=null;
					mListTaatResultOR = new LinkedList<Integer>();
					countTAATOR += getUnionForTaaT(mListTaaT1, mListTaaT2, mListTaatResultOR);
				}
				writer.write("TaatAnd");writer.write('\n');
				for(int i=0;i<queryTerm.length;i++) {
					writer.write(queryTerm[i]);
					if(i != queryTerm.length-1 )
						writer.write(" ");
				}
				//print TaatAnd
				writer.write('\n');
				writer.write("Results:");
				if(mListTaatResultAND.size() == 0){
					writer.write(" empty");writer.write('\n');
				}
				else {
					for(int j=0;j<mListTaatResultAND.size();j++)
						writer.write(" "+mListTaatResultAND.get(j));
					writer.write('\n');
				}
				writer.write("Number of documents in results: "+mListTaatResultAND.size());writer.write('\n');
				writer.write("Number of comparisons: "+countTAATAND);writer.write('\n');
				//print TaatOR
				writer.write("TaatOr");writer.write('\n');
				for(int i=0;i<queryTerm.length;i++) {
					writer.write(queryTerm[i]);
					if(i != queryTerm.length-1 )
						writer.write(" ");
				}
				writer.write('\n');
				writer.write("Results:");
				if(mListTaatResultOR.size() == 0) {
					writer.write(" empty");writer.write('\n');
				}
				else {
					for(int j=0;j<mListTaatResultOR.size();j++)
						writer.write(" "+mListTaatResultOR.get(j));
					writer.write('\n');
				}
				writer.write("Number of documents in results: "+mListTaatResultOR.size());writer.write('\n');
				writer.write("Number of comparisons: "+countTAATOR);writer.write('\n');
				//print DaatAnd
				mListDaatResultAND=null;
				mListDaatResultAND = new LinkedList<Integer>();
				countDAATAND = getIntersectionForDaaT(queryTerm, mListDaatResultAND);
				writer.write("DaatAnd");writer.write('\n');
				for(int i=0;i<queryTerm.length;i++) {
					writer.write(queryTerm[i]);
					if(i != queryTerm.length-1 )
						writer.write(" ");
				}
				writer.write('\n');
				writer.write("Results:");
				if(mListDaatResultAND.size() == 0) {
					writer.write(" empty");writer.write('\n');
				}
				else {
					for(int j=0;j<mListDaatResultAND.size();j++)
						writer.write(" "+mListDaatResultAND.get(j));
					writer.write('\n');
				}
				writer.write("Number of documents in results: "+mListDaatResultAND.size());writer.write('\n');
				writer.write("Number of comparisons: "+countDAATAND);writer.write('\n');
				//print DaatOr
				mListDaatResultOR=null;
				mListDaatResultOR = new LinkedList<Integer>();
				countDAATOR = getUnionForDaaT(queryTerm, mListDaatResultOR);
				writer.write("DaatOr");writer.write('\n');
				for(int i=0;i<queryTerm.length;i++) {
					writer.write(queryTerm[i]);
					if(i != queryTerm.length-1 )
						writer.write(" ");
				}
				writer.write('\n');
				writer.write("Results:");
				if(mListDaatResultOR.size() == 0) {
					writer.write(" empty");writer.write('\n');
				}
				else {
					for(int j=0;j<mListDaatResultOR.size();j++)
						writer.write(" "+mListDaatResultOR.get(j));
					writer.write('\n');
				}
				writer.write("Number of documents in results: "+mListDaatResultOR.size());writer.write('\n');
				writer.write("Number of comparisons: "+countDAATOR);writer.write('\n');
			}
			writer.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * utility function to compare two numbers
	 * @param a number 1
	 * @param b number 2
	 * @return 0 or 1 or 2
	 */
	static int myCompare(int a, int b) {
		if(a == b)
			return 0;
		else if(a<b)
			return 1;
		else
			return 2;
	}
	
	/**
	 * Performs intersection of postings of all queryterms using DAAT method and stores the output in mListDaatResultAND
	 * All the postings are iterated concurrently. That is 1st elements of all the postings are compared before moving onto next element
	 * of any postings. As soon as any of the postings list is completed, the function returns the results as there can be no more common terms possible 
	 * @param queryTerm array of queryterm
	 * @param mListDaatResultAND  stores the result
	 * @return counter   number of comparisons
	 */
	private static int getIntersectionForDaaT(String[] queryTerm, LinkedList<Integer> mListDaatResultAND) {
		int i=0,j=0,counter=0, result;
		Boolean flag = false;
		LinkedList<Integer> mPostingList = null;
		ArrayList<LinkedList<Integer>> mArray = new ArrayList<LinkedList<Integer>>();
		for(String s:queryTerm) {
			mPostingList = new LinkedList<Integer>();
			mPostingList = (LinkedList<Integer>) mHashMap.get(s).clone();
			mArray.add(mPostingList);
		}
		if(queryTerm.length == 1) {
			LinkedList<Integer> temp = (LinkedList<Integer>) mHashMap.get(queryTerm[0]).clone();
			for(i=0;i<temp.size();i++)
				mListDaatResultAND.add(temp.get(i));
			return 0;
		}
		while(!isEmpty(mArray)) {
			for(i=0,j=1; i<mArray.size()-1 && j<mArray.size();) {
				result = myCompare(mArray.get(i).get(0), mArray.get(j).get(0));
				counter++;
				if(result == 0) {
					flag = true;
					i++;j++;
				} else {
					if(result == 1) {
						mArray.get(i).remove(0);
					}
					else {
						mArray.get(j).remove(0);
					}
					flag = false;
					break;
				}
			}
			if(flag == true) {
				mListDaatResultAND.add(mArray.get(0).get(0));
				for(i=0; i<mArray.size(); i++) {
					mArray.get(i).remove(0);
				}
			}
		}
		return counter;
	}
	
	/**
	 * Performs union of postings of all queryterms using DAAT method and stores the output in mListDaatResultOR
	 * All the postings are iterated concurrently till all postings list are completed. Duplicate are entered only once in result.
	 * @param queryTerm array of queryterms
	 * @param mListDaatResultOR stores result of the operation
	 * @return counter stores number of comparisons
	 */
	private static int getUnionForDaaT(String[] queryTerm, LinkedList<Integer> mListDaatResultOR) {
		int i=0,j=0,counter=0, result;
		LinkedList<Integer> mPostingList = null;
		ArrayList<LinkedList<Integer>> mArray = new ArrayList<LinkedList<Integer>>();
		for(String s:queryTerm) {
			mPostingList = new LinkedList<Integer>();
			mPostingList = (LinkedList<Integer>) mHashMap.get(s).clone();
			mArray.add(mPostingList);
		}
		if(queryTerm.length == 1) {
			LinkedList<Integer> temp = (LinkedList<Integer>) mHashMap.get(queryTerm[0]).clone();
			for(i=0;i<temp.size();i++)
				mListDaatResultOR.add(temp.get(i));
			return 0;
		}
		while(!isCompleteEmpty(mArray)) {
			for(i=0,j=1; i<mArray.size()-1 && j<mArray.size();) {
				counter++;
				result = myCompare(mArray.get(i).get(0), mArray.get(j).get(0));
				if(result == 0) {
					mArray.get(i).remove(0);
					i = j;j++;
				} else {
					if(result == 1) {
						j=j+1;
					}
					else {
						i = j;j++;
					}
				}
			}
			mListDaatResultOR.add(mArray.get(i).get(0));
			mArray.get(i).remove(0);
		}
		while(mArray != null && mArray.size() != 0 && mArray.get(0) != null && mArray.get(0).size() != 0) {
			mListDaatResultOR.add(mArray.get(0).get(0));
			mArray.get(0).remove(0);
		}
		return counter;
	}

	/**
	 * Utility function to check if all the postings are completed
	 * @param mArray Array of postings
	 * @return true or false
	 */
	private static boolean isCompleteEmpty(ArrayList<LinkedList<Integer>> mArray) {
		for(int i=0;i<mArray.size();i++) {
			if(mArray.get(i).size() == 0) {
				mArray.remove(i);
				i--;
			}
		}
		if(mArray.size() == 1 || mArray.size() == 0)
			return true;
		return false;
	}

	/**
	 * utility function to check if any postings is completed
	 * @param mArray Array of postings
	 * @return true or false
	 */
	private static boolean isEmpty(ArrayList<LinkedList<Integer>> mArray) {
		for(int i=0;i<mArray.size();i++) {
			if(mArray.get(i).size() == 0)
				return true;
		}
		return false;
	}

	/**
	 * It generates intersection of two postings list using TAAT and stores intermediate result in mListTaatResult 
	 * which is input for next iterations. The loop run till iteration in any of the postings is completed.
	 * @param mListTaaT1 intermediate result before this terms
	 * @param mListTaaT2 new term postings
	 * @param mListTaatResult Stores new intermediate result (intersection of mListTaaT1 and mListTaaT2)
	 * @return counter stores number of comparisons
	 */
	private static int getIntersectionForTaaT(LinkedList<Integer> mListTaaT1, LinkedList<Integer> mListTaaT2,
			LinkedList<Integer> mListTaatResult) {
		int i=0,j=0,counter=0;
		int result;
		while(i<mListTaaT1.size() && j<mListTaaT2.size()) {
			counter++;
			result = myCompare(mListTaaT1.get(i),mListTaaT2.get(j));
			if(result == 0) {
				mListTaatResult.add(mListTaaT1.get(i));
				i++;j++;
			} else if(result == 1)
				i++;
			else
				j++;
		}
		return counter;
	}
	
	/**
	 * It generates union of two postings list using TAAT and stores intermediate result in mListTaatResult 
	 * which is input for next iterations
	 * @param mListTaaT1  intermediate result
	 * @param mListTaaT2  new term postings
	 * @param mListTaatResult stores the intermediate result (union of mListTaaT1, mListTaaT2)
	 * @return counter stores number of comparisons
	 */
	private static int getUnionForTaaT(LinkedList<Integer> mListTaaT1, LinkedList<Integer> mListTaaT2,
			LinkedList<Integer> mListTaatResult) {
		int i=0,j=0,counter=0,result;
		while(i<mListTaaT1.size() && j<mListTaaT2.size()) {
			counter++;
			result = myCompare(mListTaaT1.get(i),mListTaaT2.get(j));
			if(result == 0) {
				mListTaatResult.add(mListTaaT1.get(i));
				i++;j++;
			} else if(result == 1) {
				mListTaatResult.add(mListTaaT1.get(i));
				i++;
			}
			else {
				mListTaatResult.add(mListTaaT2.get(j));
				j++;
			}
		}
		while(i<mListTaaT1.size()) {
			mListTaatResult.add(mListTaaT1.get(i));
			i++;
		}
		while(j<mListTaaT2.size()) {
			mListTaatResult.add(mListTaaT2.get(j));
			j++;
		}
		return counter;
	}
}
