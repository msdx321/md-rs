#[derive(Debug, Clone, Default)]
pub struct AppData {
    pub chat: Vec<ChatData>,
    pub downloaded_file_ids: Vec<(String, u64)>,
}

#[derive(Debug, Clone, Default)]
pub struct ChatData {
    pub chat_id: String,
    pub ids_to_retry: Vec<i32>,
    pub last_read_message_id: i32,
}
