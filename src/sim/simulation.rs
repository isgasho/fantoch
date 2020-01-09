use crate::client::Client;
use crate::command::{Command, CommandResult};
use crate::id::{ClientId, ProcessId};
use crate::protocol::{Process, ToSend};
use crate::time::SysTime;
use std::cell::{Ref, RefCell, RefMut};
use std::collections::HashMap;

pub struct Simulation<P: Process> {
    processes: HashMap<ProcessId, (RefCell<P>, RefCell<P::Executor>)>,
    clients: HashMap<ClientId, RefCell<Client>>,
}

impl<P> Simulation<P>
where
    P: Process,
{
    /// Create a new `Simulation`.
    #[allow(clippy::new_without_default)]
    pub fn new() -> Self {
        Simulation {
            processes: HashMap::new(),
            clients: HashMap::new(),
        }
    }

    /// Registers a `Process` in the `Simulation` by storing it in a `Cell`.
    pub fn register_process(&mut self, process: P, executor: P::Executor) {
        // get identifier
        let id = process.id();

        // register process and check it has never been registered before
        let res = self
            .processes
            .insert(id, (RefCell::new(process), RefCell::new(executor)));
        assert!(res.is_none());
    }

    /// Registers a `Client` in the `Simulation` by storing it in a `Cell`.
    pub fn register_client(&mut self, client: Client) {
        // get identifier
        let id = client.id();

        // register client and check it has never been registerd before
        let res = self.clients.insert(id, RefCell::new(client));
        assert!(res.is_none());
    }

    /// Starts all clients registered in the router.
    pub fn start_clients(
        &self,
        time: &dyn SysTime,
    ) -> Vec<(ClientId, Option<(ProcessId, Command)>)> {
        self.clients
            .iter()
            .map(|(_, client)| {
                let mut client = client.borrow_mut();
                // start client
                let submit = client.start(time);
                (client.id(), submit)
            })
            .collect()
    }

    /// Forward a `ToSend`.
    pub fn forward_to_processes(&self, to_send: ToSend<P::Message>) -> Vec<ToSend<P::Message>> {
        // extract `ToSend` arguments
        let ToSend { from, target, msg } = to_send;
        target
            .into_iter()
            .filter_map(|process_id| {
                let (mut process, _) = self.get_process_mut(process_id);
                process.handle(from, msg.clone())
            })
            .collect()
    }

    /// Forward a `CommandResult`.
    pub fn forward_to_client(
        &self,
        cmd_result: CommandResult,
        time: &dyn SysTime,
    ) -> Option<(ProcessId, Command)> {
        let client_id = cmd_result.rifl().source();
        self.get_client_mut(client_id).handle(cmd_result, time)
    }

    /// Returns a reference to the process registered with this identifier.
    /// It panics if the process is not registered.
    pub fn get_process(&self, process_id: ProcessId) -> (Ref<P>, Ref<P::Executor>) {
        let (process, executor) = self.process(process_id);
        (process.borrow(), executor.borrow())
    }

    /// Returns a reference to the client registered with this identifier.
    /// It panics if the client is not registered.
    pub fn get_client(&self, client_id: ClientId) -> Ref<Client> {
        self.client(client_id).borrow()
    }

    /// Returns a mutable reference to the process registered with this identifier.
    /// It panics if the process is not registered.
    pub fn get_process_mut(&self, client_id: ClientId) -> (RefMut<P>, RefMut<P::Executor>) {
        let (process, executor) = self.process(client_id);
        (process.borrow_mut(), executor.borrow_mut())
    }

    /// Returns a mutable reference to the client registered with this identifier.
    /// It panics if the client is not registered.
    pub fn get_client_mut(&self, client_id: ClientId) -> RefMut<Client> {
        self.client(client_id).borrow_mut()
    }

    fn process(&self, process_id: ProcessId) -> &(RefCell<P>, RefCell<P::Executor>) {
        self.processes.get(&process_id).unwrap_or_else(|| {
            panic!("process {} should have been registered before", process_id);
        })
    }

    fn client(&self, client_id: ClientId) -> &RefCell<Client> {
        self.clients.get(&client_id).unwrap_or_else(|| {
            panic!("client {} should have been registered before", client_id);
        })
    }
}
