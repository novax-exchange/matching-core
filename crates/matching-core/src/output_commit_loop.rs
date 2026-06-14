use crate::journal::{OutputJournal, OutputJournalError};
use crate::output_committer::OutputCommitter;
use crate::output_queue::OutputQueue;

pub fn run_output_commit_step(
    committer: &mut OutputCommitter,
    queue: &mut OutputQueue,
    journal: &mut dyn OutputJournal,
    max_requests: usize,
) -> Result<usize, OutputJournalError> {
    let requests = queue.drain_batch(max_requests);
    let mut remaining = requests.into_iter();
    let mut committed = 0;

    while let Some(request) = remaining.next() {
        match committer.commit_one(request.clone(), journal) {
            Ok(()) => committed += 1,
            Err(error) => {
                let mut to_prepend = vec![request];
                to_prepend.extend(remaining);
                queue.prepend_requests(to_prepend);
                return Err(error);
            }
        }
    }

    Ok(committed)
}
