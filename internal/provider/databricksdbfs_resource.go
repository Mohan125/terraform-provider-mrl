package provider

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/hashicorp/terraform-plugin-framework/path"
	"github.com/hashicorp/terraform-plugin-framework/resource"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/planmodifier"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/stringplanmodifier"
	"github.com/hashicorp/terraform-plugin-framework/types"
)

// Ensure the implementation satisfies the expected interfaces.
// Ensure the implementation satisfies the expected interfaces.
var (
	_ resource.Resource                = &DatabricksDbfsResource{}
	_ resource.ResourceWithConfigure   = &DatabricksDbfsResource{}
	_ resource.ResourceWithImportState = &DatabricksDbfsResource{}
)

// NewcontainerResource is a helper function to simplify the provider implementation.
func NewDatabricksDbfsResource() resource.Resource {
	return &DatabricksDbfsResource{}

}

// orderResource is the resource implementation.
type DatabricksDbfsResource struct {
	credential *azidentity.ClientSecretCredential
}

// ImportState implements resource.ResourceWithImportState.
func (*DatabricksDbfsResource) ImportState(ctx context.Context, req resource.ImportStateRequest, resp *resource.ImportStateResponse) {

	resource.ImportStatePassthroughID(ctx, path.Root("id"), req, resp)
}

type databricksDbfsResourceModel struct {
	AdbId        types.String `tfsdk:"adb_id"`
	Token        types.String `tfsdk:"token"`
	LocalPath    types.String `tfsdk:"local_path"`
	DbfsPath     types.String `tfsdk:"dbfs_path"`
	FileSize     types.Int64  `tfsdk:"file_size"`
	LastModified types.String `tfsdk:"modification_time"`
	Md5Hash      types.String `tfsdk:"content_md5"`
}
type createRequestBody struct {
	Path      string `json:"path"`
	Contents  string `json:"contents"`
	Overwrite string `json:"overwrite"`
}
type fileUploadStatusResponseModel struct {
	Path         string `json:"path"`
	IsDirectory  bool   `json:"is_dir"`
	FileSize     int64  `json:"file_size"`
	LastModified int    `json:"modification_time"`
}

// Configure adds the provider configured client to the resource.
func (r *DatabricksDbfsResource) Configure(_ context.Context, req resource.ConfigureRequest, resp *resource.ConfigureResponse) {
	if req.ProviderData == nil {
		return
	}

	credential, ok := req.ProviderData.(*azidentity.ClientSecretCredential)
	if !ok {
		resp.Diagnostics.AddError(
			"Unexpected Data Source Configure Type",
			fmt.Sprintf("Expected *hashicups.Client, got: %T. Please report this issue to the provider developers.", req.ProviderData),
		)

		return
	}

	r.credential = credential
}

// Metadata returns the resource type name.
func (r *DatabricksDbfsResource) Metadata(_ context.Context, req resource.MetadataRequest, resp *resource.MetadataResponse) {
	resp.TypeName = req.ProviderTypeName + "_databricks_dbfs"
}

// Schema defines the schema for the resource.
func (r *DatabricksDbfsResource) Schema(_ context.Context, _ resource.SchemaRequest, resp *resource.SchemaResponse) {
	resp.Schema = schema.Schema{
		Attributes: map[string]schema.Attribute{
			"adb_id": schema.StringAttribute{
				Required:    true,
				Sensitive:   true,
				Description: "URL of the azure databricks instance",
			},
			"token": schema.StringAttribute{
				Required:    true,
				Sensitive:   true,
				Description: "Access token for the azure databricks instance",
			},
			"local_path": schema.StringAttribute{
				Required:    true,
				Description: "Local path from where the file needs to be read",
			},
			"dbfs_path": schema.StringAttribute{
				Optional: true,
				Computed: true,
				PlanModifiers: []planmodifier.String{
					stringplanmodifier.UseStateForUnknown(),
				},
				Description: "Path in dbfs where the file should be uploaded",
			},
			"file_size": schema.Int64Attribute{
				Optional:    true,
				Computed:    true,
				Description: "Size of the file being managed",
				//Default:  int64default.StaticInt64(1),
			},
			"modification_time": schema.StringAttribute{
				Optional:    true,
				Computed:    true,
				Description: "Last modified time of the file being managed",
				//Default:  stringdefault.StaticString("null"),
			},
			"content_md5": schema.StringAttribute{
				Required:    true,
				Description: "md5 hash of the file",
			},
		},
	}
}

// Create a new resource.
func (r *DatabricksDbfsResource) Create(ctx context.Context, req resource.CreateRequest, resp *resource.CreateResponse) {

	// Retrieve values from plan
	var plan databricksDbfsResourceModel
	diags := req.Plan.Get(ctx, &plan)
	resp.Diagnostics.Append(diags...)
	if resp.Diagnostics.HasError() {
		return
	}

	adburl := plan.AdbId.ValueString()
	token := plan.Token.ValueString()

	localPath := plan.LocalPath.ValueString()
	uploadEndpoint := fmt.Sprintf("%v/api/2.0/dbfs/put", adburl)
	pingEndpoint := fmt.Sprintf("%v/api/2.0/dbfs/get-status?path=", adburl)

	_, err := FileUpload(localPath, uploadEndpoint, token)
	if err != nil {
		fmt.Println(err)
	}

	fileInfo, err := FileStatus(localPath, pingEndpoint, token)
	if err != nil {
		fmt.Println(err)
	}

	plan.DbfsPath = types.StringValue(fileInfo.Path)
	plan.FileSize = types.Int64Value(fileInfo.FileSize)
	plan.LastModified = types.StringValue(time.UnixMilli(int64(fileInfo.LastModified)).UTC().Format(time.RFC3339))
	fmt.Println(plan)
	diags = resp.State.Set(ctx, plan)
	resp.Diagnostics.Append(diags...)
	if resp.Diagnostics.HasError() {
		return
	}

}

func FileStatus(fp string, p string, t string) (fileUploadStatusResponseModel, error) {

	fileInfo, err := os.Lstat(fp)
	if err != nil {
		return fileUploadStatusResponseModel{}, fmt.Errorf("file read failed")
	}

	libpath := fmt.Sprintf("/FileStore/jars/init-libs/%v", fileInfo.Name())

	httpClient := http.Client{}
	httpRequest, err := http.NewRequest("GET", fmt.Sprintf("%v%v", p, libpath), nil)
	if err != nil {
		return fileUploadStatusResponseModel{}, fmt.Errorf("request creation failed")
	}

	httpRequest.Header = http.Header{
		"Authorization": {fmt.Sprintf("Bearer %v", t)},
	}

	httpResponse, err := httpClient.Do(httpRequest)
	if err != nil {
		return fileUploadStatusResponseModel{}, fmt.Errorf("request call failed")
	}

	httpResponseBody, err := io.ReadAll(httpResponse.Body)
	if err != nil {
		return fileUploadStatusResponseModel{}, fmt.Errorf("read response body failed")
	}

	var pingResponse fileUploadStatusResponseModel

	err = json.Unmarshal(httpResponseBody, &pingResponse)
	if err != nil {
		return fileUploadStatusResponseModel{}, fmt.Errorf("unmarshal failed")
	}

	return pingResponse, nil

}
func FileUpload(fp string, e string, t string) (bool, error) {

	fileInfo, err := os.Lstat(fp)

	if err != nil {
		return false, err
	}

	fileContent, err := os.ReadFile(fp)

	fmt.Println(err)
	encodedString := base64.StdEncoding.EncodeToString(fileContent)

	jsonbody := createRequestBody{
		Path:      fmt.Sprintf("/FileStore/jars/init-libs/%v", fileInfo.Name()),
		Contents:  encodedString,
		Overwrite: "true",
	}

	jsonData, err := json.Marshal(jsonbody)

	fmt.Println(err)

	httpClient := http.Client{}

	httpRequest, err := http.NewRequest("POST", e, bytes.NewBuffer(jsonData))
	if err != nil {
		fmt.Println(err)
		return false, err
	}

	httpRequest.Header.Set("Authorization", fmt.Sprintf("Bearer %v", t))

	httpRequest.Header.Set("Content-Type", "application/json")

	httpResponse, err := httpClient.Do(httpRequest)
	if err != nil {
		return false, err
	}

	defer httpResponse.Body.Close()
	fmt.Println(httpResponse)
	if httpResponse.StatusCode == 200 {
		return true, nil
	}

	return false, nil

}

func FileDelete(fp string, e string, t string) (bool, error) {

	fileInfo, err := os.Lstat(fp)

	if err != nil {
		return false, err
	}

	jsonBody := struct {
		Path      string `json:"path"`
		Recursive bool   `json:"recursive"`
	}{
		Path:      fmt.Sprintf("/FileStore/jars/init-libs/%v", fileInfo.Name()),
		Recursive: false,
	}

	jsonData, err := json.Marshal(jsonBody)

	if err != nil {
		return false, fmt.Errorf("json marshal failed")
	}

	httpClient := http.Client{}
	httpRequest, _ := http.NewRequest("POST", e, bytes.NewBuffer(jsonData))

	httpRequest.Header.Set("Authorization", fmt.Sprintf("Bearer %v", t))

	httpRequest.Header.Set("Content-Type", "application/json")

	httpResponse, err := httpClient.Do(httpRequest)

	if httpResponse.StatusCode != 200 || err != nil {

		return false, fmt.Errorf("api call failed")
	}

	defer httpResponse.Body.Close()
	fmt.Println(httpResponse)
	if httpResponse.StatusCode == 200 {
		return true, nil
	}

	return false, err

}

// Read refreshes the Terraform state with the latest data.
func (r *DatabricksDbfsResource) Read(ctx context.Context, req resource.ReadRequest, resp *resource.ReadResponse) {

	var state databricksDbfsResourceModel
	diags := req.State.Get(ctx, &state)
	resp.Diagnostics.Append(diags...)
	if resp.Diagnostics.HasError() {
		return
	}

	adburl := state.AdbId.ValueString()
	token := state.Token.ValueString()

	localPath := state.LocalPath.ValueString()
	pingEndpoint := fmt.Sprintf("%v/api/2.0/dbfs/get-status?path=", adburl)

	fileInfo, err := FileStatus(localPath, pingEndpoint, token)
	if err != nil {
		fmt.Println(err)
	}

	state.DbfsPath = types.StringValue(fileInfo.Path)
	state.FileSize = types.Int64Value(fileInfo.FileSize)
	state.LastModified = types.StringValue(time.UnixMilli(int64(fileInfo.LastModified)).UTC().Format(time.RFC3339))
	fmt.Println(state)
	diags = resp.State.Set(ctx, &state)
	resp.Diagnostics.Append(diags...)
	if resp.Diagnostics.HasError() {
		return
	}

}

// Update updates the resource and sets the updated Terraform state on success.
func (r *DatabricksDbfsResource) Update(ctx context.Context, req resource.UpdateRequest, resp *resource.UpdateResponse) {

	var plan databricksDbfsResourceModel
	diags := req.Plan.Get(ctx, &plan)
	resp.Diagnostics.Append(diags...)
	if resp.Diagnostics.HasError() {
		return
	}

	adburl := plan.AdbId.ValueString()
	token := plan.Token.ValueString()

	localPath := plan.LocalPath.ValueString()
	uploadEndpoint := fmt.Sprintf("%v/api/2.0/dbfs/put", adburl)
	pingEndpoint := fmt.Sprintf("%v/api/2.0/dbfs/get-status?path=", adburl)

	_, err := FileUpload(localPath, uploadEndpoint, token)
	if err != nil {
		fmt.Println(err)
	}

	fileInfo, err := FileStatus(localPath, pingEndpoint, token)
	if err != nil {
		fmt.Println(err)
	}

	plan.DbfsPath = types.StringValue(fileInfo.Path)
	plan.FileSize = types.Int64Value(fileInfo.FileSize)
	plan.LastModified = types.StringValue(time.UnixMilli(int64(fileInfo.LastModified)).UTC().Format(time.RFC3339))
	fmt.Println(plan)
	diags = resp.State.Set(ctx, plan)
	resp.Diagnostics.Append(diags...)
	if resp.Diagnostics.HasError() {
		return
	}

}

// Delete deletes the resource and removes the Terraform state on success.
func (r *DatabricksDbfsResource) Delete(ctx context.Context, req resource.DeleteRequest, resp *resource.DeleteResponse) {

	var state databricksDbfsResourceModel
	diags := req.State.Get(ctx, &state)
	resp.Diagnostics.Append(diags...)
	if resp.Diagnostics.HasError() {
		return
	}

	adburl := state.AdbId.ValueString()
	token := state.Token.ValueString()

	localPath := state.LocalPath.ValueString()
	deleteEndpoint := fmt.Sprintf("%v/api/2.0/dbfs/delete", adburl)

	isOK, err := FileDelete(localPath, deleteEndpoint, token)
	if err != nil && !isOK {
		fmt.Println(err)
		panic(fmt.Errorf("delete failed"))

	}

}
