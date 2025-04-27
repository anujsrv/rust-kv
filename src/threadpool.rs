use std::thread;
use std::sync::{mpsc, Arc, Mutex};

type Job = Box<dyn FnOnce() + Send + 'static>;

pub struct Worker {
    thread: thread::JoinHandle<()>,
}

pub struct ThreadPool {
    workers: Vec<Worker>,
    sender: Option<mpsc::Sender<Job>>,
}

impl Worker {
    fn new(receiver: Arc<Mutex<mpsc::Receiver<Job>>>) -> Worker {
        let thread = thread::Builder::new().spawn(move || loop {
            match receiver.lock().unwrap().recv() {
                Ok(job) => {
                    job();
                },
                Err(_) => {
                },
            }
        }).unwrap();
        Worker{
            thread,
        }
    }
}

impl ThreadPool {
    pub fn new(size: usize) -> ThreadPool {
        let (sender, receiver) = mpsc::channel();
        let receiver = Arc::new(Mutex::new(receiver));

        let mut workers = Vec::with_capacity(size);

        for _ in 0..size {
            workers.push(Worker::new(Arc::clone(&receiver)));
        }
        ThreadPool{
            workers,
            sender: Some(sender),
        }
    }

    pub fn execute<F>(&self, f: F)
    where
        F: FnOnce() + Send + 'static,
    {
        let job = Box::new(f);
        self.sender
            .as_ref()
            .map(|sender| sender.send(job))
            .transpose()
            .unwrap();
    }
}

impl Drop for ThreadPool {
    fn drop(&mut self) {
        drop(self.sender.take());
        
        for worker in self.workers.drain(..) {
            worker.thread.join().unwrap();
        }
    }
}
