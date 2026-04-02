    async def create_folder(
        self, name: str = "新建文件夹", parent_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        name: str - 文件夹名称
        parent_id: str - 父文件夹id, 默认创建到根目录

        创建文件夹
        """
        url = f"https://{self.PIKPAK_API_HOST}/drive/v1/files"
        data = {
            "kind": "drive#folder",
            "name": name,
            "parent_id": parent_id,
        }
        result = await self._request_post(url, data)
        return result


# --- restore ---
    async def restore(
        self, share_id: str, pass_code_token: str, file_ids: List[str]
    ) -> Dict[str, Any]:
        """

        Args:
            share_id: 分享链接eg. /s/VO8BcRb-XXXXX 的 VO8BcRb-XXXXX
            pass_code_token: get_share_info获取, 无密码则留空
            file_ids: 需要转存的文件/文件夹ID列表, get_share_info获取id值
        """
        data = {
            "share_id": share_id,
            "pass_code_token": pass_code_token,
            "file_ids": file_ids,
        }
        result = await self._request_post(
            url=f"https://{self.PIKPAK_API_HOST}/drive/v1/share/restore", data=data
        )
        return result


# --- _request_post ---
    async def _request_post(
        self,
        url: str,
        data: dict = None,
        headers: dict = None,
    ):
        return await self._make_request("post", url, data=data, headers=headers)
