package azkaban.jobs;

import org.joda.time.DateTime;
import org.joda.time.Period;
import org.joda.time.PeriodType;
import org.joda.time.format.DateTimeFormat;

import azkaban.common.web.GuiUtils;
import azkaban.flow.FlowExecutionHolder;

public class QueuedJob {

	private final FlowExecutionHolder holder;
    private final JobExecution queuedJob;
	private final JobExecutorManager jobExecutionManager;
	private final DateTime queuedTime;
	
	public QueuedJob(FlowExecutionHolder holder, JobExecution queuedJob, JobExecutorManager jobExecManager) {

		this.holder = holder;
		this.queuedJob = queuedJob;
		this.jobExecutionManager = jobExecManager;
		this.queuedTime = new DateTime();
	}
	
	public JobExecutorManager getJobExecutorManager() {
		return this.jobExecutionManager;
	}
	
	public FlowExecutionHolder getFlowExecutionHolder() {
		return this.holder;
	}
	
	public String getName() {
		
		return holder.getFlow().getName();
	}
	
	public String getId() {
		return holder.getFlow().getId();
	}
	
	public JobExecution getQueuedJob() {
		return this.queuedJob;
	}
	
	public String getTime() {
		return DateTimeFormat.forPattern("MM-dd-yyyy HH:mm:ss").print(queuedTime);
	}
	
	public String getPeriod() {
		GuiUtils guiUtils = new GuiUtils();
		return guiUtils.formatPeriod(new Period(queuedTime, new DateTime(), PeriodType.seconds()));
	}
}