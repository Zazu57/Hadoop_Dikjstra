/* A rendre en janvier */

import java.io.*;
import java.util.*;
import java.text.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Dikjstra {

	// Déclaration de la matrice représentant le graphe
	public static class Matrice {
		public static final Map<String, Integer> matrice; 

		static {
			Map<String, Integer> tempMat = new HashMap<String, Integer>();
			tempMat.put("AB",3);
			tempMat.put("AC",4);
			tempMat.put("AD",1);
			tempMat.put("BE",2);
			tempMat.put("CG",5);
			tempMat.put("DF",1);
			tempMat.put("EG",3);
			tempMat.put("FC",3);
			matrice = Collections.unmodifiableMap(tempMat);
		}
	}	

	/* PASSE 1 */

	public static class TokenizerMapper extends Mapper<Object, Text, Text, Text> {
		  
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

			String valueString = value.toString().trim();

			//Récupère les lettres
			String sommet = valueString.substring(1,2);
			String voisins = valueString.substring(3).trim();
			voisins.replace("{","");
			voisins.replace("}","");

			String [] voisinsTab = voisins.split(",");
			String result ="";
			if (valueString.substring(2,3) == "0") {
				for (int i = 0; i<voisinsTab.length; i++) {
					String matriceKey = sommet+voisinsTab[i];
					Integer distance = Matrice.matrice.get(matriceKey);
					result = distance + ",{}";
				}	
			} else {
					result = "~,{"+voisins+"}";
			}			
			
			System.out.println(sommet+" "+result);

			context.write(new Text(sommet), new Text(result));

		}		
	}

	public static class IntSumReducer extends Reducer<Text,Text,Text,Text> {

		public void reduce(Text key, Text values, Context context) throws IOException, InterruptedException {
			
			System.out.println(key+" "+values);
			context.write(key, values);				
		}
	}

	// /* PASSE 2 */

	// public static class TokenizerMapper2 extends Mapper<Object, Text, Text, Text>{
		  
	// 	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

	// 		String chaine = value.toString();
	// 		String couple = chaine.substring(0,5), degree = chaine.substring(6,12);
	// 		context.write(new Text(couple), new Text(degree));
	// 	}		
	// }

	// public static class IntSumReducer2 extends Reducer<Text,Text,Text,Text> {
		
	// 	private Text result = new Text();

	// 	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			
	// 		// Concat le nombre d'enregistrements avec une key pour un couple de sommets
	// 		String degrees = "";

	// 		for (Text val : values) 
	// 		{
	// 			degrees += val.toString();
	// 			degrees += ", ";
	// 		}

	// 		result.set(degrees);
	// 		context.write(key, result);				
	// 	}
	// }

	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: dikjstra <input> <output>");
			System.exit(2);
		}
		Job job = new Job(conf, "dikjstra");
		job.setJarByClass(Dikjstra.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

		// int temp = (job.waitForCompletion(true) ? 0 : 1);

		// Job job2 = new Job(conf, "dikjstra");
		// job2.setJarByClass(Dikjstra.class);
		// job2.setMapperClass(TokenizerMapper2.class);
		// job2.setCombinerClass(IntSumReducer2.class);
		// job2.setReducerClass(IntSumReducer2.class);
		// job2.setOutputKeyClass(Text.class);
		// job2.setOutputValueClass(Text.class);
	
		// FileInputFormat.addInputPath(job2, new Path(otherArgs[1] + "/part-r-00000"));
		// FileOutputFormat.setOutputPath(job2, new Path(otherArgs[1] + "Final"));
		// System.exit(job2.waitForCompletion(true) ? 0 : 1);
	}
}
