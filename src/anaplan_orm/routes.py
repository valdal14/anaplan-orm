class AnaplanRouter:
    """A centralized router for building Anaplan API endpoint URLs."""

    def upload_file_url_builder(self, workspace_id: str, model_id: str, file_id: str) -> str:
        """
        Constructs the specific endpoint path for an Anaplan file.

        Args:
            workspace_id: Anaplan's workspace id as string
            model_id: Anaplan's destination model id as string
            file_id: Anaplan's destination file id as string

        Returns:
            str: The constructed Anaplan URL path.
        """
        return f"/workspaces/{workspace_id}/models/{model_id}/files/{file_id}"

    def process_url_builder(self, workspace_id: str, model_id: str, process_id: str) -> str:
        """
        Constructs the specific endpoint path for an Anaplan process action.

        Args:
            workspace_id: Anaplan's workspace id as string
            model_id: Anaplan's destination model id as string
            process_id: Anaplan's destination process id as string

        Returns:
            str: The constructed Anaplan URL path.
        """
        return f"/workspaces/{workspace_id}/models/{model_id}/processes/{process_id}/tasks"

    def process_task_url_builder(
        self, workspace_id: str, model_id: str, process_id: str, task_id: str
    ) -> str:
        """
        Constructs the specific endpoint path for an Anaplan process task verification.

        Args:
            workspace_id: Anaplan's workspace id as string
            model_id: Anaplan's destination model id as string
            process_id: Anaplan's destination process id as string
            task_id: Anaplan's task id used to check request status

        Returns:
            str: The constructed Anaplan URL path.
        """
        return (
            f"/workspaces/{workspace_id}/models/{model_id}/processes/{process_id}/tasks/{task_id}"
        )

    def file_chunk_url_builder(
        self, workspace_id: str, model_id: str, file_id: str, chunk_id: str
    ) -> str:
        """
        Constructs the specific endpoint path for an Anaplan chunk upload API.

        Args:
            workspace_id: Anaplan's workspace id as string
            model_id: Anaplan's destination model id as string
            file_id: Anaplan's destination file id as string
            chunk_id: Index of the chunk to be uploaded

        Returns:
            str: The constructed Anaplan URL path.
        """
        return f"/workspaces/{workspace_id}/models/{model_id}/files/{file_id}/chunks/{chunk_id}"

    def file_complete_url_builder(self, workspace_id: str, model_id: str, file_id: str) -> str:
        """
        Constructs the specific endpoint path to complete a chunked file upload.

        Args:
            workspace_id: Anaplan's workspace id as string
            model_id: Anaplan's destination model id as string
            file_id: Anaplan's destination file id as string

        Returns:
            str: The constructed Anaplan URL path.
        """
        return f"/workspaces/{workspace_id}/models/{model_id}/files/{file_id}/complete"

    def export_url_builder(self, workspace_id: str, model_id: str, export_id: str) -> str:
        """
        Constructs the specific endpoint path for an Anaplan export API.

        Args:
            workspace_id: Anaplan's workspace id as string
            model_id: Anaplan's destination model id as string
            export_id: Anaplan's destination export id as string

        Returns:
            str: The constructed Anaplan URL path.
        """
        return f"/workspaces/{workspace_id}/models/{model_id}/exports/{export_id}/tasks"

    def export_task_url_builder(
        self, workspace_id: str, model_id: str, export_id: str, task_id: str
    ) -> str:
        """
        Constructs the specific endpoint path for an Anaplan task API.

        Args:
            workspace_id: Anaplan's workspace id as string
            model_id: Anaplan's destination model id as string
            export_id: Anaplan's destination export id as string
            task_id: Anaplan's task id used to check request status

        Returns:
            str: The constructed Anaplan URL path.
        """
        return f"/workspaces/{workspace_id}/models/{model_id}/exports/{export_id}/tasks/{task_id}"

    def file_info_url_builder(self, workspace_id: str, model_id: str, file_id: str) -> str:
        """
        Constructs the specific endpoint path for an Anaplan file API.

        Args:
            workspace_id: Anaplan's workspace id as string
            model_id: Anaplan's destination model id as string
            file_id: Anaplan's destination file id as string

        Returns:
            str: The constructed Anaplan URL path.
        """
        return f"/workspaces/{workspace_id}/models/{model_id}/files/{file_id}"

    def file_chunk_list_url_builder(self, workspace_id: str, model_id: str, file_id: str) -> str:
        """
        Constructs the specific endpoint path for an Anaplan chunks API.

        Args:
            workspace_id: Anaplan's workspace id as string
            model_id: Anaplan's destination model id as string
            file_id: Anaplan's destination file id as string

        Returns:
            str: The constructed Anaplan URL path.
        """
        return f"/workspaces/{workspace_id}/models/{model_id}/files/{file_id}/chunks"
