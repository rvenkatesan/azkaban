package azkaban.jobs;

import java.io.File;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Queue;
import java.util.UUID;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import azkaban.app.JobDescriptor;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.joda.time.format.PeriodFormat;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;

import azkaban.app.AppCommon;
import azkaban.app.JobManager;
import azkaban.app.Mailman;
import azkaban.common.utils.Props;
import azkaban.common.utils.Utils;
import azkaban.flow.ExecutableFlow;
import azkaban.flow.FlowCallback;
import azkaban.flow.FlowExecutionHolder;
import azkaban.flow.FlowManager;
import azkaban.util.process.ProcessFailureException;

public class JobExecutorManager {
    private static Logger logger = Logger.getLogger(JobExecutorManager.class);
    private final Mailman mailman;
    private final JobManager jobManager;
    private final String jobSuccessEmail;
    private final String jobFailureEmail;
    private Properties runtimeProps = null;
    private final FlowManager allKnownFlows;
    private final ThreadPoolExecutor executor;
 
    // keeps track of executing <jobName> and <list of instanceIds>
    private final Multimap<String, String> executingJobs;
    // keeps track of <instanceId> and its <ExecutingJobAndInstance> 
    private final Map<String, ExecutingJobAndInstance> executingInsts;
    // Queuing related: keeps track of <jobName> and <list of <QueuingJobAndInstance>> 
    private final Map<String, Queue<QueuedJob>> queued;
    // keeps track of completed <jobName> and <list of <JobExecution>>   
    private final Multimap<String, JobExecution> completed;
    
    @SuppressWarnings("unchecked")
	public JobExecutorManager(
    		FlowManager allKnownFlows,
    		JobManager jobManager,
    		Mailman mailman,                     
    		String jobSuccessEmail,
            String jobFailureEmail,
            int maxThreads) {
    	
    	this.jobManager = jobManager;
    	this.mailman = mailman;
    	this.jobSuccessEmail = jobSuccessEmail;
    	this.jobFailureEmail = jobFailureEmail;
    	this.allKnownFlows = allKnownFlows;
    	
        Multimap<String, JobExecution> typedMultiMap = HashMultimap.create();
        this.completed = Multimaps.synchronizedMultimap(typedMultiMap);
        
        Multimap<String, String> execMultiMap = HashMultimap.create();
        this.executingJobs = Multimaps.synchronizedMultimap(execMultiMap);
        this.executingInsts = new ConcurrentHashMap<String, ExecutingJobAndInstance>();
        
    	this.queued = new ConcurrentHashMap<String, Queue<QueuedJob>> ();
    	
    	this.executor = new ThreadPoolExecutor(0, maxThreads, 10, TimeUnit.SECONDS, new LinkedBlockingQueue(), new ExecutorThreadFactory());
    } 
    
    /**
     * Cancels an specific instance of an already running job.
     * 
     * @param name
     * @throws Exception
     */

    public void cancel(String jobId) throws Exception {
    	
    	logger.info("Cancelling job-id [" + jobId + "]");
        ExecutingJobAndInstance jobInst = executingInsts.get(jobId);
    
        if(jobInst == null) {
            throw new IllegalArgumentException("[" + jobId + "] is not currently running.");
        }
        else {
        	JobExecution runningJob = jobInst.getScheduledJob();
        	ExecutableFlow flow = jobInst.getExecutableFlow();
        	logger.info("Cancelling job [" + runningJob.getId() + "] id=[" + jobId + "] time=[" + new DateTime().getMillis() + "]");
        	flow.cancel();
        	
        	// TODO: superfluous; will be taken care by the flow.cancel()
        	//serviceQueuedJob(runningJob);
        	//removeExecutingJob(runningJob);
        }
    }

    /**
     * Run job file given the id
     * 
     * @param id
     * @param ignoreDep
     */
    public void execute(String id, boolean ignoreDep) {

    	final ExecutableFlow flowToRun = allKnownFlows.createNewExecutableFlow(id);

    	if (isExecuting(id) && !canQueue(id)) {
    			throw new JobExecutionException("Job " + id + " is already running.");
    	}
    	
        if(ignoreDep) {
            for(ExecutableFlow subFlow: flowToRun.getChildren()) {
                subFlow.markCompleted();
            }
        }
        execute(flowToRun);
    }
         
    /**
     * Runs the job immediately
     * 
     * @param holder The execution of the flow to run
     */
    public void execute(ExecutableFlow flow) {
    	if (isExecuting(flow.getName()) && !canQueue(flow.getName())) {
    		throw new JobExecutionException("Job " + flow.getName() + " is already running.");
    	}
    	
        final Props parentProps = produceParentProperties(flow);
        FlowExecutionHolder holder = new FlowExecutionHolder(flow, parentProps);
        execute(holder);        
    }
   
    /**
     * Schedule this flow to run one time at the specified date
     * 
     * @param holder The execution of the flow to run
     */
    public void execute(FlowExecutionHolder holder) {
    	
        ExecutableFlow flow = holder.getFlow();
        
        // case-1: max allowed instances are already executing and can not queue => throw exception (drop instances)   
    	if (isExecuting(flow.getName()) && !canQueue(flow.getName())) 
    		throw new JobExecutionException("Job [" + flow.getName() + "] is already running and cannot queue!.");
        
    	final DateTime ts = new DateTime();
        final JobExecution executingJob = new JobExecution(flow.getName(), flow.getId(), ts, true);
        
        // case-2: max allowed instances are already executing, but can queue
        if (isExecuting(flow.getName()) && canQueue(flow.getName())) {
        	
        	addToQueue(new QueuedJob(holder, executingJob, this));
        }
        // case-3: max allowed instances not reached yet
        else {
        	// case-3a: see if anything is left in the queue; if so, queue the new one and execute the queued ones (rare case)
        	if (getQueueSize(executingJob.getId()) > 0) {
        		addToQueue(new QueuedJob(holder, executingJob, this));
        		while(isExecuting(executingJob.getId()))
        			serviceQueuedJob(executingJob);
        	}
        	// case-3b: none in the queue; go ahead and execute
        	else {
        		logger.info("Executing job=[" + flow.getName() + "] instanceid=[" + flow.getId() + "] time=[" + ts.getMillis() + "]");
        		ExecutingFlowRunnable execFlowRunnable = new ExecutingFlowRunnable(holder, executingJob);        	
        		executor.execute(execFlowRunnable);
        	}
        }
    }
    
    /**
     * set runtime properties
     * 
     * @param p
     */
    public void setRuntimeProperties(Properties p) {
        runtimeProps = p;
    }

    /**
     * get runtime property
     * 
     * @param name property name
     * @return property value
     */
    public String getRuntimeProperty(String name) {
        return (runtimeProps == null) ? null : runtimeProps.getProperty(name);
    }

    /**
     * set runtime property
     * 
     * @param name property name
     * @param value property value
     */
    public void setRuntimeProperty(String name, String value) {
        if(runtimeProps == null) {
            runtimeProps = new Properties();
        }
        runtimeProps.setProperty(name, value);
    }

    
    /*
     * Wrap a single exception with the name of the scheduled job
     */
    private void sendErrorEmail(JobExecution job,
                                Throwable exception,
                                String senderAddress,
                                List<String> emailList) {
        Map<String, Throwable> map = new HashMap<String, Throwable>();
        map.put(job.getId(), exception);
        sendErrorEmail(job, map, senderAddress, emailList);
    }
    

    /*
     * Send error email
     * 
     * @param job scheduled job
     * 
     * @param exceptions exceptions thrown by failed jobs
     * 
     * @param senderAddress email address of sender
     * 
     * @param emailList email addresses of receivers
     */
    private void sendErrorEmail(JobExecution job,
                                Map<String, Throwable> exceptions,
                                String senderAddress,
                                List<String> emailList) {
        if((emailList == null || emailList.isEmpty()) && jobFailureEmail != null)
            emailList = Arrays.asList(jobFailureEmail);

        if(emailList != null && mailman != null) {
            try {

                StringBuffer body = new StringBuffer("The job '"
                                                     + job.getId()
                                                     + "' running on "
                                                     + InetAddress.getLocalHost().getHostName()
                                                     + " has failed with the following errors: \r\n\r\n");
                int errorNo = 1;
                String logUrlPrefix = runtimeProps != null ? runtimeProps.getProperty(AppCommon.DEFAULT_LOG_URL_PREFIX)
                                                           : null;
                if(logUrlPrefix == null && runtimeProps != null) {
                    logUrlPrefix = runtimeProps.getProperty(AppCommon.LOG_URL_PREFIX);
                }

                final int lastLogLineNum = 60;
                for(Map.Entry<String, Throwable> entry: exceptions.entrySet()) {
                    final String jobId = entry.getKey();
                    final Throwable exception = entry.getValue();

                    /* append job exception */
                    String error = (exception instanceof ProcessFailureException) ? ((ProcessFailureException) exception).getLogSnippet()
                                                                                 : Utils.stackTrace(exception);
                    body.append(" Job " + errorNo + ". " + jobId + ":\n" + error + "\n");

                    /* append log file link */
                    JobExecution jobExec = jobManager.loadMostRecentJobExecution(jobId);
                    if(jobExec == null) {
                        body.append("Job execution object is null for jobId:" + jobId + "\n\n");
                    }

                    String logPath = jobExec != null ? jobExec.getLog() : null;
                    if(logPath == null) {
                        body.append("Log path is null. \n\n");
                    } else {
                        body.append("See log in " + logUrlPrefix + logPath + "\n\n" + "The last "
                                    + lastLogLineNum + " lines in the log are:\n");

                        /* append last N lines of the log file */
                        String logFilePath = this.jobManager.getLogDir() + File.separator
                                             + logPath;
                        Vector<String> lastNLines = Utils.tail(logFilePath, 60);

                        if(lastNLines != null) {
                            for(String line: lastNLines) {
                                body.append(line + "\n");
                            }
                        }
                    }

                    errorNo++;
                }

                // logger.error("\n\n error email body: \n" + body.toString() +
                // "\n");

                mailman.sendEmailIfPossible(senderAddress,
                                             emailList,
                                             "Job '" + job.getId() + "' has failed!",
                                             body.toString());

            } catch(UnknownHostException uhe) {
                logger.error(uhe);
            }
        }
    }

    private void sendSuccessEmail(JobExecution job,
                                  Duration duration,
                                  String senderAddress,
                                  List<String> emailList) {
        if((emailList == null || emailList.isEmpty()) && jobSuccessEmail != null) {
            emailList = Arrays.asList(jobSuccessEmail);
        }

        if(emailList != null && mailman != null) {
            try {
                mailman.sendEmailIfPossible(senderAddress,
                                             emailList,
                                             "Job '" + job.getId() + "' has completed on "
                                                     + InetAddress.getLocalHost().getHostName()
                                                     + "!",
                                             "The job '"
                                                     + job.getId()
                                                     + "' completed in "
                                                     + PeriodFormat.getDefault()
                                                                   .print(duration.toPeriod())
                                                     + ".");
            } catch(UnknownHostException uhe) {
                logger.error(uhe);
            }
        }
    }
    
    private Props produceParentProperties(final ExecutableFlow flow) {
        Props parentProps = new Props();

        parentProps.put("azkaban.flow.id", flow.getId());
        parentProps.put("azkaban.flow.uuid", UUID.randomUUID().toString());

        DateTime loadTime = new DateTime();

        parentProps.put("azkaban.flow.start.timestamp", loadTime.toString());
        parentProps.put("azkaban.flow.start.year", loadTime.toString("yyyy"));
        parentProps.put("azkaban.flow.start.month", loadTime.toString("MM"));
        parentProps.put("azkaban.flow.start.day", loadTime.toString("dd"));
        parentProps.put("azkaban.flow.start.hour", loadTime.toString("HH"));
        parentProps.put("azkaban.flow.start.minute", loadTime.toString("mm"));
        parentProps.put("azkaban.flow.start.seconds", loadTime.toString("ss"));
        parentProps.put("azkaban.flow.start.milliseconds", loadTime.toString("SSS"));
        parentProps.put("azkaban.flow.start.timezone", loadTime.toString("ZZZZ"));
        return parentProps;
    }
    
    /**
     * A thread factory that sets the correct classloader for the thread
     */
    public class ExecutorThreadFactory implements ThreadFactory {

        private final AtomicInteger threadCount = new AtomicInteger(0);

        public Thread newThread(Runnable r) {
            Thread t = new Thread(r);
            t.setDaemon(true);
            t.setName("scheduler-thread-" + threadCount.getAndIncrement());
            return t;
        }
    }
    
	
    public class ExecutingJobAndInstance {
    	
        private final ExecutableFlow flow;
        private final JobExecution scheduledJob;

        private ExecutingJobAndInstance(ExecutableFlow flow, JobExecution scheduledJob) {
            this.flow = flow;
            this.scheduledJob = scheduledJob;
        }

        public ExecutableFlow getExecutableFlow() {
            return flow;
        }

        public JobExecution getScheduledJob() {
            return scheduledJob;
        }                
    }
    
//    public class QueuingJobAndInstance extends ExecutingJobAndInstance {
//    	
//    	private final JobExecutorManager jobExecutionManager;
//    	private final FlowExecutionHolder holder;
//    	private final long queuedTime;
//    	
//    	private QueuingJobAndInstance(FlowExecutionHolder holder, JobExecution queuedJob, JobExecutorManager jobExecManager) {
//
//    		super(holder.getFlow(), queuedJob);
//    		this.jobExecutionManager = jobExecManager;
//    		this.holder = holder;
//    		this.queuedTime = new DateTime().getMillis();
//    	}
//    	
//    	public JobExecutorManager getJobExecutorManager() {
//    		return this.jobExecutionManager;
//    	}
//    	
//    	public FlowExecutionHolder getFlowExecutionHolder() {
//    		return this.holder;
//    	}
//    	
//    	public String getJobName() {
//    		return getExecutableFlow().getName();
//    	}
//    	
//    	public String getJobId() {
//    		return getExecutableFlow().getId();
//    	}
//    	
//    	public String getQueuedTime() {
//    		return DateTimeFormat.forPattern("MM-dd-yyyy HH:mm:ss").print(queuedTime);
//    	}	
//    }
   
    /**
     * A runnable adapter for a Job
     */
    private class ExecutingFlowRunnable implements Runnable {

        private final JobExecution runningJob;
        private final FlowExecutionHolder holder;

        private ExecutingFlowRunnable(FlowExecutionHolder holder, JobExecution runningJob) {
            this.holder = holder;
            this.runningJob = runningJob;
        }

        public void run() {
            final ExecutableFlow flow = holder.getFlow();
            logger.info("Starting run of " + flow.getName());

            List<String> emailList = null;
            String senderAddress = null;
            try {
                final JobDescriptor jobDescriptor = jobManager.getJobDescriptor(flow.getName());

                emailList = jobDescriptor.getEmailNotificationList();
                final List<String> finalEmailList = emailList;

                senderAddress = jobDescriptor.getSenderEmail();
                final String senderEmail = senderAddress;

                // mark the job as executing by adding it to executingJobs list
                runningJob.setStartTime(new DateTime()); 
                executingInsts.put(flow.getId(), new ExecutingJobAndInstance(flow, runningJob));          
                executingJobs.put(flow.getName(), flow.getId());
                                
                flow.execute(holder.getParentProps(), new FlowCallback() {

                    @Override
                    public void progressMade() {
                        allKnownFlows.saveExecutableFlow(holder);                      
                    }

                    @Override
                    public void completed(Status status) {
                    	runningJob.setEndTime(new DateTime());

                        try {
                            allKnownFlows.saveExecutableFlow(holder);
                            switch(status) {
                                case SUCCEEDED: 
                                    if (jobDescriptor.getSendSuccessEmail()) {
                                        sendSuccessEmail(
                                                runningJob,
                                                runningJob.getExecutionDuration(),
                                                senderEmail,
                                                finalEmailList
                                        );
                                    }
                                    break;
                                case FAILED:
                                    sendErrorEmail(runningJob,
                                                   flow.getExceptions(),
                                                   senderEmail,
                                                   finalEmailList);
                                    break;
                                default:
                                    sendErrorEmail(runningJob,
                                                   new RuntimeException(String.format("Got an unknown status[%s]",
                                                                                      status)),
                                                   senderEmail,
                                                   finalEmailList);
                            }
                        } catch(RuntimeException e) {
                            logger.warn("Exception caught while saving flow/sending emails", e);   
                            serviceQueuedJob(runningJob);
                            removeFromExecutingJobs(runningJob);
                            throw e;
                        } finally {
                            // mark the job as completed and remove from executing list
                        	// executing.remove(runningJob.getId());
                        	serviceQueuedJob(runningJob);
                        	removeFromExecutingJobs(runningJob);
                            completed.put(runningJob.getId(), runningJob);
                        }
                    }
                });
                allKnownFlows.saveExecutableFlow(holder);
            } catch(Throwable t) {
            	serviceQueuedJob(runningJob);
            	removeFromExecutingJobs(runningJob);
            	if(emailList != null) {
                    sendErrorEmail(runningJob, t, senderAddress, emailList);
                }
                logger.warn(String.format("An exception almost made it back to the ScheduledThreadPool from job[%s]",
                		runningJob),
                            t);
            }
        }
    }
    
    public boolean isExecuting(String jobName) {
    	
        if (executingJobs.containsKey(jobName)) {        	
        	int maxParallelRuns = jobManager.getJobDescriptor(jobName).getProps().getInt(JobDescriptor.JOB_MAX_PARALLEL_RUNS, 1);
        	if ((maxParallelRuns != -1) && (executingJobs.get(jobName).size() >= maxParallelRuns)) {
        		return true;
        	}
        	else
        		return false;
        }
        else {
        	return false;
        }
    }
    
   public boolean canQueue(String jobName) {
    	
	   int maxQueueSize = jobManager.getJobDescriptor(jobName).getProps().getInt(JobDescriptor.JOB_QUEUE_SIZE, 0);
	   int currQueueSize = getQueueSize(jobName);
	   if ((maxQueueSize == -1) || (currQueueSize <= maxQueueSize)) 
		   return true;
	   else
		   return false;
    }
   
   public int getQueueSize(String jobName) {
	   
	   if (!queued.containsKey(jobName))
		   return 0;
	   else
		   return queued.get(jobName).size();
   }
   
   public Collection<ExecutingJobAndInstance> getExecutingJobs() {
	   return executingInsts.values();
    }

   public Multimap<String, JobExecution> getCompleted() {
        return completed;
    }
    
   public Collection<QueuedJob> getQueued() {

    	Collection<QueuedJob> queuedInstances = new ArrayList<QueuedJob>(); 
        Collection<Queue<QueuedJob>> collQueued = queued.values();
        
        for(Queue<QueuedJob> q : collQueued) {
        	Iterator<QueuedJob> itr = q.iterator();
        	while(itr.hasNext()) {
            	queuedInstances.add(itr.next());
        	}
        }
        return queuedInstances;	    	
    }

	private ThreadPoolExecutor getThreadPoolExecutor() {
		return this.executor;
	}
	
	private void addToQueue(QueuedJob execJobInst) {
		
		String jobName = execJobInst.getName();
		String jobId = execJobInst.getId();
		
		Queue<QueuedJob> qItems = queued.get(jobName);
		if (qItems == null) {
			qItems = new LinkedList<QueuedJob>();
			queued.put(jobName, qItems);
		}
		logger.info("Queuing job [" + jobName + "] instanceid=[" + jobId + "] time=[" + new DateTime().getMillis() + "]");
		qItems.add(execJobInst);
   }
	
   private void serviceQueuedJob(JobExecution runningJob) {
	   
		Queue<QueuedJob> qItems = queued.get(runningJob.getId()); // pass in the jobName
		if ((qItems != null) && (qItems.peek() != null)) {
			QueuedJob qInst = qItems.poll();
			String jobName = qInst.getName();
			String jobInstId = qInst.getId();
			logger.info("Servicing Queued job [" + jobName + "] instanceid=[" + jobInstId + "] time=[" + new DateTime().getMillis() + "]");
			qInst.getJobExecutorManager().getThreadPoolExecutor().execute(new ExecutingFlowRunnable(qInst.getFlowExecutionHolder(), qInst.getQueuedJob()));
		}	   
   }
	
   public void removeFromExecutingJobs(JobExecution runningJob) {

	   String jobName = runningJob.getId();
	   String jobId = runningJob.getInstId();
	   removeFromExecutingJobs(jobName, jobId);
   }
   
   public void removeFromExecutingJobs(String jobName, String jobId) {

       logger.info("Removing job from executing list [" + jobName + "] instanceid=[" + jobId + "] time=[" + new DateTime().getMillis() + "]");
       executingJobs.get(jobName).remove(jobId);
       executingInsts.remove(jobId);
   }
   
	public void removeJobFromQueue(String jobName, String jobId) {
		
		Queue<QueuedJob> qItems = queued.get(jobName);
		if (qItems != null) {
			QueuedJob obj = null;
			for(QueuedJob o : qItems) {
				if (o.getId().equals(jobId)) {
					obj = o;
					break;
				}
			}
			if (obj != null) {
				logger.info("Removing from Queue job [" + jobName + "] instanceid=[" + jobId + "] time=[" + new DateTime().getMillis() + "]");
				qItems.remove(obj);
			}
		}
   }
}
