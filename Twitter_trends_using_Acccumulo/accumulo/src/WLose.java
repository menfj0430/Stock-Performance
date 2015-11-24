package com;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import org.apache.accumulo.core.data.Value;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.Parser;
import org.apache.hadoop.util.Tool;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.accumulo.core.util.CachedConfiguration;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.accumulo.core.client.mapreduce.AccumuloOutputFormat;
import org.apache.accumulo.core.data.Mutation;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class WLose extends Configured implements Tool {
	private static Options opts;
	private static Option passwordOpt;
	private static Option usernameOpt;
	private static String USAGE = "wordCount <instance name> <zoo keepers> <input dir> <output table>";
	private static final String WINTABLE = "WinTable";
	private static final String LOSETABLE= "LoseTable";
	
	static {
		usernameOpt = new Option("u", "username", true, "root");
		passwordOpt = new Option("p", "password", true, "acc");

		opts = new Options();

		opts.addOption(usernameOpt);
		opts.addOption(passwordOpt);
	}

	public static class MapClass extends Mapper<LongWritable, Text, Text, Mutation> {
		private static final String REG_WHITESPACE = "\\s+";
		private static final String WIN = "win";
		private static final String LOSE= "lose";
		// @Override
		public void map(LongWritable key, Text value, Context output) throws IOException {

		HashMap<String, String> hashteammap =new HashMap<String, String>();
		List<String> east = new ArrayList<String>(Arrays.asList("Celtics", "Knicks","76ers","Nets","Raptors","Bulls","Pacers","Bucks","Pistons","Cavs","MiamiHeat","OrlandoMagic","Hawks","Bobcats","Wizards"));
	    //String[] east={"Celtics", "Knicks","76ers","Nets","Raptors","Bulls","Pacers","Bucks","Pistons","Cavs","MiamiHeat","OrlandoMagic","Hawks","Bobcats","Wizards"};
		List<String> west = new ArrayList<String>(Arrays.asList("okcthunder","Nuggets","TrailBlazers","UtahJazz","TWolves","Lakers","Suns","GSWarriors","Clippers","NBAKings","GoSpursGo","Mavs","Hornets","Grizzlies","Rockets"));
	    hashteammap.put("Celtics","Boston Celtics");hashteammap.put("Knicks","New York Knicks");		hashteammap.put("76ers","Philadelphia 76ers");		hashteammap.put("Nets","New Jersey Nets");
		hashteammap.put("Raptors","Toronto Raptors");		hashteammap.put("Bulls","Chicago Bulls");		hashteammap.put("Pacers","Indiana Pacers");		hashteammap.put("Bucks","Milwaukee Bucks");
		hashteammap.put("Pistons","Detroit Pistons");		hashteammap.put("Cavs","Cleveland Cavaliers");		hashteammap.put("MiamiHeat","Miami Heat");		hashteammap.put("OrlandoMagic","Orlando Magic");
		hashteammap.put("Hawks","Atlanta Hawks");		hashteammap.put("Bobcats","Charlotte Bobcats");		hashteammap.put("Wizards","Washington Wizards");		hashteammap.put("okcthunder","Oklahoma City");
		hashteammap.put("Nuggets","Denver Nuggets");		hashteammap.put("TrailBlazers","Portland Trailblazers");		hashteammap.put("UtahJazz","Utah Jazz");		hashteammap.put("TWolves","Minnesota Timberwolves");
		hashteammap.put("Lakers","L A Lakers");		hashteammap.put("Suns","Phoenix Suns");		hashteammap.put("GSWarriors","Golden State Warriors");		hashteammap.put("Clippers","L.A.Clippers");
		hashteammap.put("NBAKings","Sacramento Kings");	hashteammap.put("GoSpursGo","San Antonio Spurs");	hashteammap.put("Mavs","Dallas Mavericks");		hashteammap.put("Hornets","New Orleans Hornets");
		hashteammap.put("Grizzlies","Memphis Grizzlies");		hashteammap.put("Rockets","Houston Rockets");

    	
    	String filename = ((FileSplit)(output.getInputSplit())).getPath().getName();
    	filename = filename.substring(0, filename.length()-4);
    	if (filename.endsWith((".csv")))
    			{
    		filename=filename.replace(".", "");
    	}
    	/*if(filename.charAt(filename.length()-1)=='.')
    		filename = filename.substring(0,filename.length()-1);
      */
    	Mutation mut8on = null;Mutation tmpmutation = new Mutation(new Text(hashteammap.get(filename)));;
    	try {
    		tmpmutation.put(new Text("#"+filename),new Text(""),new ColumnVisibility("east"),new Value("0".getBytes()));;
    		output.write(new Text("myTable"), tmpmutation);//remove
    		} catch (InterruptedException e1) {
			e1.printStackTrace();
		}
    	int eastcount=0;int westcount=0;
    	
    	
    	if(east.contains(filename))
    	{
	    	Mutation m1 = new Mutation(new Text(hashteammap.get(filename)));
	    	Mutation m2 = new Mutation(new Text(hashteammap.get(filename)));
	    	m2.put(new Text("#"+filename),new Text(""),new ColumnVisibility("east"),new Value("0".getBytes()));
	    	m1.put(new Text("#"+filename),new Text(""),new ColumnVisibility("east"),new Value("0".getBytes()));
	    	try {
	    		eastcount++; //remove
				output.write(new Text(WINTABLE), m1);
				eastcount++;//
				output.write(new Text(LOSETABLE), m2);
				} catch (InterruptedException e1) {
				e1.printStackTrace();
			}
    	}
    	if(west.contains(filename))
    	{
    		Mutation mutation1 = new Mutation(new Text(hashteammap.get(filename)));
        	mutation1.put(new Text("#"+filename),new Text(""),new ColumnVisibility("west"),new Value("0".getBytes()));
        	Mutation mutation2=new Mutation(new Text(hashteammap.get(filename)));
        	mutation2.put(new Text("#"+filename),new Text(""),new ColumnVisibility("west"),new Value("0".getBytes()));
        	try {
        		westcount++;//
    			output.write(new Text(WINTABLE), mutation1);
    			westcount++;//
    			output.write(new Text(LOSETABLE), mutation2);
    		} catch (InterruptedException e1) {
    			e1.printStackTrace();
    		}	
    	}
    	
    	
    	String[] words = value.toString().split(REG_WHITESPACE);
    	
    	try {
    		Mutation tmpmutation1 = new Mutation(new Text(hashteammap.get(filename)));;
    		if (words.length>2){
    		tmpmutation1.put(new Text("#"+filename),new Text(""),new ColumnVisibility("east"),new Value((words[1]+" eastcount"+eastcount+" westcount"+westcount).getBytes()));;
    		output.write(new Text("myTable"), tmpmutation);//remove
    		}} catch (InterruptedException e1) {
			e1.printStackTrace();
		}
    	
    	for (String word : words) {
                       
        if(!word.equalsIgnoreCase(WIN) && !word.equalsIgnoreCase(LOSE))
        {
        	continue;        	
        }
        else{
        	
        	mut8on=new Mutation(new Text(hashteammap.get(filename)));
        	//mut8on1=new Mutation(new Text(hashteammap.get(filename)));
        	
        	if(east.contains(filename))mut8on.put(new Text("#"+filename),new Text(""),new ColumnVisibility("east"),new Value("1".getBytes()));	
        	else if(west.contains(filename)) mut8on.put(new Text("#"+filename),new Text(""),new ColumnVisibility("west"),new Value("1".getBytes()));
        	}
                        
        try {
        	if(word.equalsIgnoreCase(WIN))output.write(new Text(WINTABLE), mut8on);
        	if(word.equalsIgnoreCase(LOSE)) output.write(new Text("LoseTable"), mut8on);
        	} catch (InterruptedException e) {
        		e.printStackTrace();
        	}
      }
    }
	}
	public static class ReduceClass extends Reducer<WritableComparable, Writable, Text, Mutation> {
	    public void reduce(WritableComparable key, Iterable<Text> values, Context c) {

	        try {
		        Mutation m = null;
				c.write(new Text(WINTABLE), m);
				c.write(new Text(LOSETABLE), m);
			} catch (IOException e) {
				e.printStackTrace();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
	    }
	}
	public int run(String[] unprocessed_args) throws Exception {
		CommandLine cl=new BasicParser().parse(opts, unprocessed_args);
		String[] args=cl.getArgs();
		String username=cl.getOptionValue(usernameOpt.getOpt(), "root");
		String password=cl.getOptionValue(passwordOpt.getOpt(), "secret");

		if (args.length!=4) 
		{
			System.out.println("ERROR: Wrong number of parameters: "
					+ args.length + " instead of 4.");
			return printUsage();
		}
		Job job = new Job(getConf(), WLose.class.getName());
		
		job.setJarByClass(this.getClass());

		
		job.setInputFormatClass(TextInputFormat.class);
		TextInputFormat.setInputPaths(job, new Path(args[2]));

		job.setMapperClass(MapClass.class);

		job.setNumReduceTasks(0);

		job.setOutputFormatClass(AccumuloOutputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Mutation.class);
		AccumuloOutputFormat.setOutputInfo(job.getConfiguration(), username,password.getBytes(), true, args[3]);
		AccumuloOutputFormat.setZooKeeperInstance(job.getConfiguration(),args[0], args[1]);
		job.waitForCompletion(true);
		return 0;
	}

	private int printUsage() {
		HelpFormatter hf = new HelpFormatter();
		hf.printHelp(USAGE, opts);
		return 0;
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(CachedConfiguration.getInstance(),
				new WLose(), args);
		System.exit(res);
	}
}
